---
name: yml2ddl
description: Generate DDL from YML
---

# YML 2 DDL Skill

This skill generates SQL DDL statements from Starlake YML definitions.

## Usage

```bash
starlake yml2ddl [options]
```

## Options

- `--datawarehouse <value>`: Target datawarehouse name (required)
- `--connection <value>`: JDBC connection name
- `--output <value>`: Output path
- `--catalog <value>`: Database Catalog
- `--domain <value>`: Domain to create DDL for
- `--schemas <value>`: Schemas to create DDL for
- `--apply`: Apply DDL directly?
- `--parallelism <value>`: Parallelism level

## Examples

### Generate DDL for Snowflake

```bash
starlake yml2ddl --datawarehouse snowflake --domain mydomain
```
