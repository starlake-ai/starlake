---
name: summarize
description: Display table summary and statistics
---

# Summarize Skill

Runs a quick summary query on a table, displaying statistics like row count, column statistics, and sample data. Useful for data exploration and validation.

## Usage

```bash
starlake summarize [options]
```

## Options

- `--domain <value>`: Domain name (required)
- `--table <value>`: Table name (required)
- `--accessToken <value>`: Access token for authentication (e.g. GCP)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Summarize a Table

```bash
starlake summarize --domain starbake --table orders
```

### Summarize with Access Token

```bash
starlake summarize --domain starbake --table products --accessToken $GCP_TOKEN
```

### Summarize with JSON Output

```bash
starlake summarize --domain starbake --table order_lines --reportFormat json
```

## Related Skills

- [metrics](../metrics/SKILL.md) - Compute detailed statistical metrics
- [freshness](../freshness/SKILL.md) - Check data freshness