---
name: extract
description: Extract both schema and data from a JDBC source
---

# Extract Skill

Combines schema extraction and data extraction in a single command. First extracts the database schema metadata into Starlake YAML files, then extracts the actual data into files. This is a convenience command that runs `extract-schema` followed by `extract-data`.

## Usage

```bash
starlake extract [options]
```

## Options

Combines all options from [extract-schema](../extract-schema/SKILL.md) and [extract-data](../extract-data/SKILL.md).

### Schema Extraction Options

- `--config <value>`: Database tables & connection info
- `--outputDir <value>`: Where to output YML files
- `--tables <value>`: Database tables to extract
- `--connectionRef <value>`: JDBC connection reference
- `--all`: Extract all schemas and tables
- `--external`: Output YML files to the external folder
- `--parallelism <value>`: Parallelism level
- `--snakecase`: Apply snake_case to column names

### Data Extraction Options

- `--limit <value>`: Limit number of records
- `--numPartitions <value>`: Partition parallelism
- `--ignoreExtractionFailure`: Continue on extraction failure
- `--clean`: Clean target files before extraction
- `--incremental`: Export only new data since last extraction
- `--includeSchemas <value>`: Domains to include
- `--excludeSchemas <value>`: Domains to exclude
- `--includeTables <value>`: Tables to include
- `--excludeTables <value>`: Tables to exclude
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

Extract commands use a configuration file (`metadata/extract/{name}.sl.yml`) to define which schemas and tables to extract:

```yaml
# metadata/extract/externals.sl.yml
version: 1
extract:
  connectionRef: "duckdb"
  jdbcSchemas:
    - schema: "starbake"
      tables:
        - name: "*"              # "*" to extract all tables
      tableTypes:
        - "TABLE"
```

### Advanced Extract Configuration

```yaml
# metadata/extract/source_db.sl.yml
version: 1
extract:
  connectionRef: "source_postgres"
  jdbcSchemas:
    - schema: "sales"
      tableTypes:
        - "TABLE"
        - "VIEW"
      tables:
        - name: "orders"
          fullExport: false          # Incremental extraction
          partitionColumn: "id"      # Column for parallel extraction
          numPartitions: 4           # Parallelism level
          timestamp: "updated_at"    # Incremental tracking column
          fetchSize: 1000            # JDBC fetch size
        - name: "customers"
          fullExport: true
```

### Connection Configuration

The connection referenced in the extract config must be defined in `application.sl.yml`:

```yaml
# metadata/application.sl.yml
version: 1
application:
  connections:
    source_postgres:
      type: jdbc
      options:
        url: "jdbc:postgresql://{{PG_HOST}}:{{PG_PORT}}/{{PG_DB}}"
        driver: "org.postgresql.Driver"
        user: "{{DATABASE_USER}}"
        password: "{{DATABASE_PASSWORD}}"
```

## Examples

### Extract Schema and Data

```bash
starlake extract --config externals --outputDir metadata/load
```

### Extract with Incremental Mode

```bash
starlake extract --config source_db --outputDir /tmp/output --incremental
```

### Extract Specific Tables

```bash
starlake extract --config source_db --tables sales.orders,sales.customers
```

## Related Skills

- [extract-schema](../extract-schema/SKILL.md) - Extract schema only
- [extract-data](../extract-data/SKILL.md) - Extract data only
- [extract-script](../extract-script/SKILL.md) - Generate extraction scripts from templates
- [load](../load/SKILL.md) - Load extracted data into the warehouse