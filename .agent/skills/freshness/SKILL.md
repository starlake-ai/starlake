---
name: freshness
description: Check for data freshness
---

# Freshness Skill

This skill helps you check the freshness of your data tables.

## Usage

```bash
starlake freshness [options]
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

### Check Freshness for All Tables

```bash
starlake freshness --all
```
