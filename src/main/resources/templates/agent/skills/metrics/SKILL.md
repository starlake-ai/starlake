---
name: metrics
description: Compute statistical metrics on table data
---

# Metrics Skill

Computes statistical metrics on a table's data. Metrics are based on the `metric` type defined on each attribute: `continuous` for numeric columns (min, max, mean, stddev, etc.) and `discrete` for categorical columns (distinct count, frequency distribution).

## Usage

```bash
starlake metrics [options]
```

## Options

- `--domain <value>`: Domain name (required)
- `--schema <value>`: Table/schema name (required)
- `--authInfo k1=v1,k2=v2`: Auth info for the connection (e.g. `gcpProjectId=my-project`)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

### Attribute Metric Types

Metrics are configured per attribute in table YAML files:

```yaml
# In table.sl.yml
table:
  attributes:
    - name: "total_amount"
      type: "decimal"
      metric: "continuous"   # min, max, mean, median, variance, stddev, percentiles
    - name: "status"
      type: "string"
      metric: "discrete"     # count distinct, category frequency
    - name: "order_id"
      type: "long"
      # No metric - not computed
```

### Metric Types

| Metric Type | Computed Values |
|---|---|
| `continuous` | min, max, sum, mean, median, variance, stddev, skewness, kurtosis, 25th/75th percentiles, missing values, row count |
| `discrete` | count distinct, category frequency, top categories, row count |

### Application-Level Configuration

```yaml
# metadata/application.sl.yml
application:
  metrics:
    active: true
    discreteMaxCardinality: 10    # Max distinct values for discrete metrics
```

Metrics are stored in the `SL_METRICS` audit table for historical tracking.

## Examples

### Compute Metrics for a Table

```bash
starlake metrics --domain starbake --schema orders
```

### Compute with Auth Info

```bash
starlake metrics --domain starbake --schema orders --authInfo gcpProjectId=my-gcp-project
```

### Compute with JSON Report

```bash
starlake metrics --domain starbake --schema products --reportFormat json
```

## Related Skills

- [summarize](../summarize/SKILL.md) - Display table summary statistics
- [freshness](../freshness/SKILL.md) - Check data freshness
- [config](../config/SKILL.md) - Configuration reference (metrics settings)