---
name: ingest
description: Ingest data from specific paths into a domain/table
---

# Ingest Skill

Low-level ingestion command that loads data from specified file paths directly into a domain/table. Unlike `load` which scans directories, `ingest` targets specific files. This is useful for programmatic or API-driven ingestion scenarios.

## Usage

```bash
starlake ingest [domain] [schema] [paths] [options]
```

## Positional Arguments

- `domain`: Domain name (e.g. `starbake`)
- `schema`: Schema/table name (e.g. `orders`)
- `paths`: Comma-separated list of file paths to ingest

## Options

- `--options k1=v1,k2=v2`: Substitution arguments
- `--scheduledDate <value>`: Scheduled date for the job, format: `yyyy-MM-dd'T'HH:mm:ss.SSSZ`
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Ingest Specific Files

```bash
starlake ingest starbake orders /data/incoming/orders_20240301.json
```

### Ingest Multiple Files

```bash
starlake ingest starbake orders /data/file1.json,/data/file2.json
```

### Ingest with Options

```bash
starlake ingest starbake orders /data/orders.json --options env=prod
```

## Related Skills

- [load](../load/SKILL.md) - Directory-based data loading (more common)
- [autoload](../autoload/SKILL.md) - Automatic schema inference and loading