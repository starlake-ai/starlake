---
name: extract-bq-schema
description: Extract schemas directly from BigQuery datasets
---

# Extract BigQuery Schema Skill

Extracts table schemas directly from BigQuery datasets into Starlake YAML configuration files. Unlike `extract-schema` which uses JDBC, this command uses the BigQuery API directly for better metadata extraction.

## Usage

```bash
starlake extract-bq-schema [options]
```

## Options

- `--connection <value>`: BigQuery connection reference from `application.sl.yml`
- `--database <value>`: GCP Project ID
- `--tables <value>`: Comma-separated list of `dataset.table` pairs to extract
- `--external`: Include external datasets defined in `_config.sl.yml`
- `--write <value>`: Write mode for output: `OVERWRITE`, `APPEND`
- `--accessToken <value>`: Access token for GCP authentication
- `--persist <value>`: Persist results to files (`true`/`false`)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

Requires a BigQuery connection in `application.sl.yml`:

```yaml
# metadata/application.sl.yml
version: 1
application:
  connections:
    bigquery:
      type: "bigquery"
      options:
        location: "europe-west1"
        authType: "APPLICATION_DEFAULT"
        authScopes: "https://www.googleapis.com/auth/cloud-platform"
        writeMethod: "direct"
```

## Examples

### Extract All Schemas from a Project

```bash
starlake extract-bq-schema --database my-gcp-project
```

### Extract Specific Tables

```bash
starlake extract-bq-schema --database my-gcp-project --tables sales.orders,sales.customers
```

### Extract with Persist

```bash
starlake extract-bq-schema --database my-gcp-project --persist true
```

### Extract Including External Datasets

```bash
starlake extract-bq-schema --database my-gcp-project --external
```

## Related Skills

- [extract-schema](../extract-schema/SKILL.md) - Extract schema via JDBC
- [bq-info](../bq-info/SKILL.md) - Get BigQuery table information
- [extract](../extract/SKILL.md) - Extract both schema and data