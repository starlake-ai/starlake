---
name: metrics
description: Compute metrics
---

# Metrics Skill

This skill computes metrics for a table.

## Usage

```bash
starlake metrics [options]
```

## Options

- `--domain <value>`: Domain Name (required)
- `--schema <value>`: Schema Name (required)
- `--authInfo <value>`: Auth Info (gcpProjectId, gcpSAJsonKey)

## Examples

### Compute Metrics

```bash
starlake metrics --domain mydomain --schema mytable
```
