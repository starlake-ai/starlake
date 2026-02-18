---
name: yml2ddl
description: Generate SQL DDL statements from Starlake YAML definitions
---

# YML to DDL Skill

Generates SQL DDL (Data Definition Language) statements — CREATE TABLE, ALTER TABLE, etc. — from your Starlake YAML table definitions. Supports multiple target databases through type mappings defined in `types/default.sl.yml`.

## Usage

```bash
starlake yml2ddl [options]
```

## Options

- `--datawarehouse <value>`: Target data warehouse name — must match a DDL mapping key in `types/default.sl.yml` (required). Examples: `bigquery`, `snowflake`, `postgres`, `redshift`, `synapse`, `duckdb`
- `--connection <value>`: JDBC connection name with read/write access (for `--apply` mode)
- `--output <value>`: Output directory for generated DDL files (default: `./{datawarehouse}/`)
- `--catalog <value>`: Database catalog name (if applicable)
- `--domain <value>`: Generate DDL for this specific domain only (default: all domains)
- `--schemas <value>`: Comma-separated list of schemas within the domain to generate DDL for
- `--apply`: Execute the generated DDL directly against the database
- `--parallelism <value>`: Parallelism level (default: available CPU cores)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

### Type Mappings in `types/default.sl.yml`

The DDL generation uses `ddlMapping` to map Starlake types to database-specific types:

```yaml
# metadata/types/default.sl.yml
types:
  - name: "string"
    primitiveType: "string"
    ddlMapping:
      bigquery: "STRING"
      snowflake: "VARCHAR"
      postgres: "TEXT"
      duckdb: "VARCHAR"
      synapse: "NVARCHAR(MAX)"

  - name: "long"
    primitiveType: "long"
    ddlMapping:
      bigquery: "INT64"
      snowflake: "BIGINT"
      postgres: "BIGINT"
      duckdb: "BIGINT"

  - name: "decimal"
    primitiveType: "decimal"
    ddlMapping:
      bigquery: "NUMERIC"
      snowflake: "NUMBER(38,9)"
      postgres: "NUMERIC"
```

## Examples

### Generate DDL for BigQuery

```bash
starlake yml2ddl --datawarehouse bigquery
```

### Generate DDL for Snowflake (Specific Domain)

```bash
starlake yml2ddl --datawarehouse snowflake --domain starbake
```

### Generate and Apply DDL to PostgreSQL

```bash
starlake yml2ddl --datawarehouse postgres --connection my_pg_conn --apply
```

### Generate DDL to Custom Output Directory

```bash
starlake yml2ddl --datawarehouse duckdb --output /tmp/ddl
```

### Generate DDL for Specific Schemas

```bash
starlake yml2ddl --datawarehouse snowflake --domain sales --schemas orders,customers
```

### Parallel DDL Generation

```bash
starlake yml2ddl --datawarehouse bigquery --parallelism 4
```

## Related Skills

- [extract-script](../extract-script/SKILL.md) - Generate extraction scripts from templates
- [xls2yml](../xls2yml/SKILL.md) - Convert Excel to YAML (before DDL generation)
- [config](../config/SKILL.md) - Configuration reference (type mappings)