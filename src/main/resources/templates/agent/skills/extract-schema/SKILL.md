---
name: extract-schema
description: Extract database schemas into Starlake YAML configuration files
---

# Extract Schema Skill

Connects to a JDBC database and extracts table schemas (column names, types, constraints) into Starlake YAML configuration files. This is the first step when reverse-engineering an existing database into a Starlake project.

## Usage

```bash
starlake extract-schema [options]
```

## Options

- `--config <value>`: Extract configuration name (references a file in `metadata/extract/`)
- `--outputDir <value>`: Where to output the generated YML files
- `--tables <value>`: Specific database tables to extract
- `--connectionRef <value>`: JDBC connection reference defined in `application.sl.yml`
- `--all`: Extract all schemas and tables to the external folder
- `--external`: Output YML files in the `metadata/external/` folder
- `--parallelism <value>`: Parallelism level for extraction (default: available CPU cores)
- `--snakecase`: Apply snake_case transformation to column names
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

### Extract Configuration File (`metadata/extract/{name}.sl.yml`)

```yaml
# metadata/extract/externals.sl.yml
version: 1
extract:
  connectionRef: "duckdb"
  jdbcSchemas:
    - schema: "starbake"
      tables:
        - name: "*"              # Extract all tables
      tableTypes:
        - "TABLE"                # TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY
```

### Connection Configuration

The connection must be defined in `application.sl.yml`:

```yaml
# metadata/application.sl.yml
version: 1
application:
  connections:
    duckdb:
      type: "jdbc"
      options:
        url: "jdbc:duckdb:{{SL_ROOT}}/datasets/duckdb.db"
        driver: "org.duckdb.DuckDBDriver"
    source_postgres:
      type: jdbc
      options:
        url: "jdbc:postgresql://{{PG_HOST}}:5432/{{PG_DB}}"
        driver: "org.postgresql.Driver"
        user: "{{DATABASE_USER}}"
        password: "{{DATABASE_PASSWORD}}"
```

### Generated Output

The command generates table YAML files like:

```yaml
# Generated: metadata/load/starbake/orders.sl.yml
version: 1
table:
  name: "orders"
  pattern: "orders_.*.json"
  attributes:
    - name: "order_id"
      type: "long"
      required: true
    - name: "customer_id"
      type: "long"
    - name: "status"
      type: "string"
    - name: "timestamp"
      type: "timestamp"
```

## Examples

### Extract All Schemas from a Config

```bash
starlake extract-schema --config externals --outputDir metadata/load
```

### Extract Using a Connection Reference

```bash
starlake extract-schema --connectionRef source_postgres --outputDir metadata/load
```

### Extract Specific Tables

```bash
starlake extract-schema --config externals --tables starbake.orders,starbake.customers
```

### Extract All to External Folder

```bash
starlake extract-schema --config externals --all --external
```

### Extract with Snake Case Naming

```bash
starlake extract-schema --config externals --outputDir metadata/load --snakecase
```

### Parallel Extraction

```bash
starlake extract-schema --config externals --outputDir metadata/load --parallelism 8
```

## Related Skills

- [extract](../extract/SKILL.md) - Extract both schema and data
- [extract-data](../extract-data/SKILL.md) - Extract data from tables
- [infer-schema](../infer-schema/SKILL.md) - Infer schema from data files
- [config](../config/SKILL.md) - Configuration reference (connections, types)