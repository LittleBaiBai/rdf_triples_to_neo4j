# rdf_triples_to_neo4j
This is a tool that initializes a neo4j DB instance and import triples file listed under a given path.

## Build

```bash
GOOS=darwin GOARCH=amd64 go build -o rdf-triples-amd64  # For Intel Macs
GOOS=darwin GOARCH=arm64 go build -o rdf-triples  # For M1/M2/M3 Macs
```

## Usage

```bash
./rdf-triples /path/to/rdf/files \
    --neo4j-uri bolt://localhost:7687 \
    --username neo4j \
    --password password \
    --container-mount /uprise-outputs # optional: path where the triples are mounted in the Neo4j Docker container, leave out if you are using a local Neo4j instance \
    --schema-file /path/to/constraints.ttl # optional: path to the SHACL constraints file
```
