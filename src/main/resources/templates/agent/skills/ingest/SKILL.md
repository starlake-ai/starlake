---
name: ingest
description: Ingest data
---

# Ingest Skill

This skill allows for generic data ingestion.

## Usage

```bash
starlake ingest [options]
```

## Options

- `domain` (arg): Domain name
- `schema` (arg): Schema name
- `paths` (arg): List of comma separated paths
- `options` (arg): Arguments to be used as substitutions
- `--scheduledDate <value>`: Scheduled date for the job

## Examples

### Ingest Specific Path

```bash
starlake ingest mydomain myschema /path/to/data
```
