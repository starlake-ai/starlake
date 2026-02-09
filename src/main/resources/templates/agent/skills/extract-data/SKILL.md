---
name: extract-data
description: Extract data from a database to CSV/Parquet files
---

# Extract Data Skill

This skill helps you extract data from database tables defined in your mapping configuration.

## Usage

```bash
starlake extract-data [options]
```

## Options

- `--config <value>`: Database tables & connection info (required)
- `--outputDir <value>`: Where to output data files (required)
- `--limit <value>`: Limit number of records
- `--numPartitions <value>`: Parallelism level regarding partitioned tables
- `--parallelism <value>`: Parallelism level of the extraction process
- `--ignoreExtractionFailure`: Don't fail extraction job when any extraction fails
- `--clean`: Clean all files of table only when it is extracted
- `--incremental`: Export only new data since last extraction
- `--ifExtractedBefore <value>`: DateTime to compare with the last beginning extraction dateTime
- `--includeSchemas <value>`: Domains to include
- `--excludeSchemas <value>`: Domains to exclude
- `--includeTables <value>`: Tables to include
- `--excludeTables <value>`: Tables to exclude

## Examples

### Extract All Data

```bash
starlake extract-data --config my-config --outputDir /tmp/output
```

### Incremental Extraction

```bash
starlake extract-data --config my-config --outputDir /tmp/output --incremental
```
