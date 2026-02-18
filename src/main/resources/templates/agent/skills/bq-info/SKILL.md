---
name: bq-info
description: Get table information from BigQuery
---

# BigQuery Info Skill

Retrieves detailed information about BigQuery tables including schema, row count, size, partitioning, and clustering configuration.

## Usage

```bash
starlake bq-info [options]
```

## Options

- `--connection <value>`: BigQuery connection reference from `application.sl.yml`
- `--database <value>`: GCP Project ID
- `--tables <value>`: Comma-separated list of `dataset.table` pairs
- `--external`: Include external datasets defined in `_config.sl.yml`
- `--write <value>`: Write mode: `OVERWRITE`, `APPEND`
- `--accessToken <value>`: Access token for GCP authentication
- `--persist <value>`: Persist results (`true`/`false`)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Get Info for Specific Tables

```bash
starlake bq-info --tables sales.orders,sales.customers
```

### Get Info for All Tables in a Project

```bash
starlake bq-info --database my-gcp-project
```

### Get Info with Access Token

```bash
starlake bq-info --database my-gcp-project --tables sales.orders --accessToken $GCP_TOKEN
```

## Related Skills

- [extract-bq-schema](../extract-bq-schema/SKILL.md) - Extract BigQuery schemas to YAML
- [freshness](../freshness/SKILL.md) - Check data freshness