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

- `--input <value>`: Dataset Input Path (required)
- `--domain <value>`: Domain Name
- `--table <value>`: Table Name
- `--outputDir <value>`: Domain YAML Output Path
- `--write <value>`: Write Mode
- `--format <value>`: Force input file format
- `--rowTag <value>`: Row tag (XML)
- `--clean`: Delete previous YML before writing
- `--encoding <value>`: Input file encoding

## Examples

### Infer from CSV

```bash
starlake infer-schema --domain mydomain --table mytable --input /path/to/file.csv --format DSV
```
