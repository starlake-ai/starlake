---
name: cnxload
description: Load parquet file into a JDBC Table
---

# JDBC Load Skill

This skill loads a Parquet file into a JDBC table.

## Usage

```bash
starlake cnxload [options]
```

## Options

- `--source_file <value>`: Full Path to source file (required)
- `--output_table <value>`: JDBC Output Table (required)
- `--options <value>`: Connection options (driver, user, password, url, etc.)
- `--write_strategy <value>`: Write strategy (APPEND, OVERWRITE)

## Examples

### Load to PostgreSQL

```bash
starlake cnxload --source_file /path/to/data.parquet --output_table myschema.mytable --options url=jdbc:postgresql://...,user=...,password=...
```
