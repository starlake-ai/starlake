---
name: extract-bq-schema
description: Extract schema from BigQuery
---

# Extract BigQuery Schema Skill

This skill helps you extract schemas directly from BigQuery.

## Usage

```bash
starlake extract-bq-schema [options]
```

## Options

- `--write <value>`: One of OVERWRITE, APPEND, UPSERT_BY_KEY, etc.
- `--connection <value>`: Connection to use
- `--database <value>`: Database / Project ID
- `--external`: Include external datasets defined in \_config.sl.yml
- `--tables <value>`: List of datasetName.tableName1,datasetName.tableName2 ...
- `--accessToken <value>`: Access token to use for authentication
- `--persist <value>`: Persist results? (true/false)

## Examples

### Extract Schema for a Project

```bash
starlake extract-bq-schema --database my-gcp-project
```
