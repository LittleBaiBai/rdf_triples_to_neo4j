package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "rdf-import",
		Usage: "Import RDF files into Neo4j",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "neo4j-uri",
				Value:   "bolt://localhost:7687",
				EnvVars: []string{"NEO4J_URI"},
				Usage:   "Neo4j database URI",
			},
			&cli.StringFlag{
				Name:    "username",
				Value:   "neo4j",
				EnvVars: []string{"NEO4J_USER"},
				Usage:   "Neo4j username",
			},
			&cli.StringFlag{
				Name:    "password",
				Value:   "password",
				EnvVars: []string{"NEO4J_PASSWORD"},
				Usage:   "Neo4j password",
			},
			&cli.StringFlag{
				Name:  "schema-file",
				Usage: "Optional: Path to SHACL constraints file for validation. If not provided, validation will be skipped",
			},
			&cli.StringFlag{
				Name:  "container-mount",
				Value: "/uprise-outputs",
				Usage: "Optional: Path where the triples are mounted in the Neo4j Docker container. If provided, this path will be used instead of the local file path",
			},
			&cli.BoolFlag{
				Name:  "initialize",
				Value: true,
				Usage: "Initialize database before import",
			},
		},
		Action: importRDF,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func importRDF(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("please provide the input directory path")
	}

	inputDir := c.Args().First()
	loader, err := NewRDFLoader(c.Context, c.String("neo4j-uri"), c.String("username"), c.String("password"))
	if err != nil {
		return err
	}
	defer loader.Close()

	if c.Bool("initialize") {
		if err := loader.InitializeDatabase(); err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
		log.Println("Database initialized")
	}

	// Find all .ttl files
	files, err := filepath.Glob(filepath.Join(inputDir, "*.ttl"))
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	if len(files) == 0 {
		log.Printf("No .ttl files found in %s\n", inputDir)
		return nil
	}

	// Import each file
	containerMount := c.String("container-mount")
	for _, file := range files {
		result, err := loader.LoadTriples(file, containerMount)
		if err != nil {
			log.Printf("❌ Error importing %s: %v\n", filepath.Base(file), err)
			continue
		}
		if result.Success {
			log.Printf("✅ %s: %s\n", filepath.Base(file), result.Message)
		} else {
			log.Printf("❌ %s: %s\n", filepath.Base(file), result.Message)
		}
	}

	// Validate if schema file provided
	if schemaFile := c.String("schema-file"); schemaFile != "" {
		if err := loader.ValidateGraph(schemaFile, containerMount); err != nil {
			log.Printf("❌ Validation failed: %v\n", err)
		} else {
			log.Println("✅ Validation passed")
		}
	}

	return nil
}
