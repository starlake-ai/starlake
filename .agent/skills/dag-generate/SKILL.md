---
name: dag-generate
description: Generate DAGs
---

# DAG Generate Skill

This skill generates Airflow DAGs from your Starlake project.

## Usage

```bash
starlake dag-generate [options]
```

## Options

- `--outputDir <value>`: Output path
- `--clean`: Clean output first?
- `--tags <value>`: Generate for these tags only
- `--tasks`: Generate for tasks
- `--domains`: Generate for domains
- `--withRoles`: Generate role definitions

## Examples

### Generate All DAGs

```bash
starlake dag-generate --outputDir /path/to/output --clean
```
