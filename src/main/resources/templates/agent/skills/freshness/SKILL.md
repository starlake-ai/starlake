---
name: freshness
description: Check data freshness and last update timestamps
---

# Freshness Skill

Checks the freshness of your data tables by querying the last update timestamps. Results are stored in the `SL_LAST_EXPORT` audit table for monitoring and alerting.

## Usage

```bash
starlake freshness [options]
```

## Options

- `--connection <value>`: Connection reference from `application.sl.yml`
- `--database <value>`: Database / GCP Project ID
- `--tables <value>`: Comma-separated list of `dataset.table` pairs to check
- `--external`: Include external datasets defined in `_config.sl.yml`
- `--write <value>`: Write mode: `OVERWRITE`, `APPEND`
- `--accessToken <value>`: Access token for authentication
- `--persist <value>`: Persist results to audit tables (`true`/`false`)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

Freshness thresholds can be defined at the domain or table level:

```yaml
# In _config.sl.yml or table.sl.yml
load:
  metadata:
    freshness:
      warn: "24h"     # Warn if data is older than 24 hours
      error: "48h"    # Error if data is older than 48 hours
```

## Examples

### Check Freshness for Specific Tables

```bash
starlake freshness --tables starbake.orders,starbake.customers --persist true
```

### Check Freshness for All Tables

```bash
starlake freshness --database my-gcp-project --persist true
```

### Check Freshness with Connection

```bash
starlake freshness --connection duckdb --tables starbake.orders
```

## Related Skills

- [bq-info](../bq-info/SKILL.md) - Get BigQuery table information
- [metrics](../metrics/SKILL.md) - Compute statistical metrics on tables
- [validate](../validate/SKILL.md) - Validate project configuration