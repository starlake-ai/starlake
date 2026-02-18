---
name: extract-data
description: Extract data from database tables to CSV/Parquet files
---

# Extract Data Skill

Extracts data from database tables into local files (CSV, Parquet). Supports full and incremental extraction, parallel processing, and schema/table filtering.

## Usage

```bash
starlake extract-data [options]
```

## Options

- `--config <value>`: Extract configuration name (required) — references a file in `metadata/extract/`
- `--outputDir <value>`: Where to output data files (required)
- `--limit <value>`: Limit number of records extracted per table
- `--numPartitions <value>`: Parallelism level for partitioned table extraction
- `--parallelism <value>`: Parallelism level of the overall extraction process
- `--ignoreExtractionFailure`: Continue extraction even if individual tables fail
- `--clean`: Clean all files for a table before extracting it
- `--incremental`: Export only new data since last extraction (uses timestamp tracking)
- `--ifExtractedBefore <value>`: Only extract if last extraction was before this datetime
- `--includeSchemas <value>`: Comma-separated list of schemas/domains to include
- `--excludeSchemas <value>`: Comma-separated list of schemas/domains to exclude
- `--includeTables <value>`: Comma-separated list of tables to include
- `--excludeTables <value>`: Comma-separated list of tables to exclude
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

### Extract Configuration

```yaml
# metadata/extract/externals.sl.yml
version: 1
extract:
  connectionRef: "source_postgres"
  jdbcSchemas:
    - schema: "sales"
      tables:
        - name: "orders"
          fullExport: false
          partitionColumn: "id"
          numPartitions: 4
          timestamp: "updated_at"
          fetchSize: 1000
        - name: "customers"
          fullExport: true
        - name: "*"               # All remaining tables
```

## Examples

### Full Data Extraction

```bash
starlake extract-data --config externals --outputDir /tmp/output
```

### Incremental Extraction

Extract only rows added/updated since last extraction:

```bash
starlake extract-data --config externals --outputDir /tmp/output --incremental
```

### Extract with Record Limit

```bash
starlake extract-data --config externals --outputDir /tmp/output --limit 1000
```

### Extract Specific Schemas

```bash
starlake extract-data --config externals --outputDir /tmp/output --includeSchemas sales,hr
```

### Extract Excluding Specific Tables

```bash
starlake extract-data --config externals --outputDir /tmp/output --excludeTables audit_log,temp_data
```

### Parallel Extraction with Fault Tolerance

```bash
starlake extract-data --config externals --outputDir /tmp/output --parallelism 8 --ignoreExtractionFailure
```

### Clean Before Extraction

```bash
starlake extract-data --config externals --outputDir /tmp/output --clean
```

## Related Skills

- [extract](../extract/SKILL.md) - Extract both schema and data
- [extract-schema](../extract-schema/SKILL.md) - Extract schema metadata only
- [load](../load/SKILL.md) - Load extracted data into the warehouse