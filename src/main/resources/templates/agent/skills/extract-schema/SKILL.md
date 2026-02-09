---
name: extract-schema
description: Extract schema from a database to Starlake YAML files
---

# Extract Schema Skill

This skill helps you extract database schemas into Starlake's YAML configuration format.

## Usage

```bash
starlake extract-schema [options]
```

## Options

- `--config <value>`: Database tables & connection info (required if not using valid default)
- `--outputDir <value>`: Where to output YML files
- `--parallelism <value>`: Parallelism level of the extraction process
- `--tables <value>`: Database tables info (list of tables)
- `--connectionRef <value>`: Database connection to use (JDBC connection reference)
- `--all`: Extract all schemas and tables to external folder
- `--external`: Output YML files in the external folder
- `--snakecase`: Apply snake case when name sanitization is done

## Configuration

The extraction is often driven by a configuration file.
The schema for this configuration is defined in `utils/resources/starlake.json` under `ExtractSchemaConfig` (or similar, verifying...).

## Examples

### Basic Extraction

```bash
starlake extract-schema --config my-details --outputDir /path/to/output
```
