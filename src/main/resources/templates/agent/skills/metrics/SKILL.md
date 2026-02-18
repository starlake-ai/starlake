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
- `--schema <value>`: Schema/Table Name (required)
- `--authInfo <value>`: Auth Info to use for the connection (e.g. gcpProjectId, gcpSAJsonKey)

## Examples

### Compute Metrics for a Table

Compute metrics for the `customers` table in the `starbake` domain.

```bash
starlake metrics --domain starbake --schema customers
```
