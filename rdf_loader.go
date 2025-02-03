package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type ImportResult struct {
	Success bool
	Message string
}

type RDFLoader struct {
	driver neo4j.DriverWithContext
	ctx    context.Context
}

func NewRDFLoader(ctx context.Context, uri, username, password string) (*RDFLoader, error) {
	driver, err := connectToNeo4j(uri, username, password)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Neo4j: %w", err)
	}
	return &RDFLoader{
		driver: driver,
		ctx:    ctx,
	}, nil
}

func (l *RDFLoader) Close() {
	l.driver.Close(l.ctx)
}

func connectToNeo4j(uri, username, password string) (neo4j.DriverWithContext, error) {
	auth := neo4j.NoAuth()
	if username != "" && password != "" {
		auth = neo4j.BasicAuth(username, password, "")
	}
	return neo4j.NewDriverWithContext(uri, auth)
}

func (l *RDFLoader) InitializeDatabase() error {
	session := l.driver.NewSession(l.ctx, neo4j.SessionConfig{})
	defer session.Close(l.ctx)

	queries := []string{
		"MATCH (n) DETACH DELETE n;",
		"CALL n10s.graphconfig.init({handleVocabUris: 'IGNORE'});",
		"CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE;",
	}

	for _, query := range queries {
		if _, err := session.Run(l.ctx, query, map[string]any{}); err != nil {
			return err
		}
	}
	return nil
}

func (l *RDFLoader) getFileURI(filePath string, containerMount string) (string, error) {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return "", err
	}

	if containerMount != "" {
		absPath = filepath.Join(containerMount, filepath.Base(absPath))
	}

	return "file://" + strings.ReplaceAll(absPath, "\\", "/"), nil
}

func (l *RDFLoader) LoadTriples(filePath string, containerMount string) (ImportResult, error) {
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		session := l.driver.NewSession(l.ctx, neo4j.SessionConfig{})
		defer session.Close(l.ctx)

		fileURI, err := l.getFileURI(filePath, containerMount)
		if err != nil {
			return ImportResult{}, err
		}

		query := fmt.Sprintf(
			"CALL n10s.rdf.import.fetch('%s', 'Turtle', {nodeCacheSize: 15000});",
			fileURI,
		)

		result, err := session.Run(l.ctx, query, map[string]any{})
		if err != nil {
			lastErr = err
			log.Printf("Attempt %d/%d failed: %v\n", attempt+1, maxRetries, err)
			time.Sleep(time.Second)
			continue
		}

		record, err := result.Single(l.ctx)
		if err != nil {
			return ImportResult{}, err
		}

		triplesLoaded, _ := record.Get("triplesLoaded")
		if triplesLoaded.(int64) > 0 {
			return ImportResult{
				Success: true,
				Message: fmt.Sprintf("%d triples loaded", triplesLoaded),
			}, nil
		}

		extraInfo, exists := record.Get("extraInfo")
		if exists {
			return ImportResult{
				Success: false,
				Message: fmt.Sprintf("No triples loaded. Extra info: %v", extraInfo),
			}, nil
		}
	}

	return ImportResult{}, fmt.Errorf("failed after %d attempts: %v", maxRetries, lastErr)
}

func (l *RDFLoader) ValidateGraph(schemaFile string, containerMount string) error {
	session := l.driver.NewSession(l.ctx, neo4j.SessionConfig{})
	defer session.Close(l.ctx)

	fileURI, err := l.getFileURI(schemaFile, containerMount)
	if err != nil {
		return err
	}

	queries := []string{
		"CALL n10s.validation.shacl.dropShapes();",
		fmt.Sprintf("CALL n10s.validation.shacl.import.fetch('%s','Turtle');", fileURI),
		"CALL n10s.validation.shacl.validate() yield focusNode, nodeType, offendingValue, resultPath, resultMessage, severity;",
	}

	for i, query := range queries {
		result, err := session.Run(l.ctx, query, map[string]any{})
		if err != nil {
			return err
		}

		if i == 1 && !result.Next(l.ctx) {
			return fmt.Errorf("failed to load constraints file")
		}

		if i == 2 {
			hasViolations := false
			for result.Next(l.ctx) {
				record := result.Record()
				log.Printf("Validation error: %v\n", record.Values[5])
				hasViolations = true
			}
			if hasViolations {
				return fmt.Errorf("SHACL validation failed")
			}
		}
	}

	return nil
}
