---
name: infer-schema
description: Infer schema from a file
---

# Infer Schema Skill

This skill infers a Starlake schema from a data file (CSV, JSON, etc.).

## Usage

```bash
starlake infer-schema [options]
```

## Options

- `--input <value>`: Dataset Input Path (required). Can be a file or a directory.
- `--domain <value>`: Domain Name (e.g. `starbake`)
- `--table <value>`: Table Name (e.g. `ingredients`)
- `--outputDir <value>`: Domain YAML Output Path (default: `metadata/domains`)
- `--write <value>`: Write Mode (OVERWRITE, APPEND)
- `--format <value>`: Force input file format (DSV, JSON, XML, PARQUET, etc.)
- `--rowTag <value>`: Row tag to wrap records (XML only)
- `--clean`: Delete previous YML file before writing
- `--encoding <value>`: Input file encoding (default: UTF-8)
- `--from-json-schema`: Input file is a valid JSON Schema file

## Examples

### Infer from CSV

Infer the schema for the `ingredients` table in the `starbake` domain from a CSV file.

```bash
starlake infer-schema --domain starbake --table ingredients --input /path/to/ingredients.csv --format DSV
```

### Infer from JSON Schema

Infer the schema for the `orders` table in the `starbake` domain from a JSON Schema file.

```bash
starlake infer-schema --domain starbake --table orders --input /path/to/orders_schema.json --from-json-schema
```
