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

- `--source_file <value>`: Full Path to source file (required). Supports Parquet, CSV, JSON.
- `--output_table <value>`: JDBC Output Table (required). format: schema.table
- `--driver <value>`: JDBC Driver class name (e.g. org.postgresql.Driver)
- `--url <value>`: JDBC URL (e.g. jdbc:postgresql://localhost:5432/mydb)
- `--user <value>`: Database user
- `--password <value>`: Database password
- `--options <value>`: Connection options (key=value,key2=value2)
- `--write_strategy <value>`: Write strategy (APPEND, OVERWRITE, TRUNCATE, ERROR_IF_EXISTS)
- `--create_table_if_not_exists`: Create table if it does not exist

## Examples

### Load Parquet to PostgreSQL

Load a parquet file into the `public.mytable` table in a PostgreSQL database.

```bash
starlake cnxload \
  --source_file /path/to/data.parquet \
  --output_table public.mytable \
  --driver org.postgresql.Driver \
  --url jdbc:postgresql://localhost:5432/mydb \
  --user myuser \
  --password mypassword \
  --write_strategy APPEND
```
