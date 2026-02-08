---
name: dag-deploy
description: Deploy DAGs
---

# DAG Deploy Skill

This skill deploys generated DAGs to a target directory.

## Usage

```bash
starlake dag-deploy [options]
```

## Options

- `--inputDir <value>`: Folder containing generated DAGs
- `--outputDir <value>`: Target deployment directory (required)
- `--dagDir <value>`: Sub-directory for DAG files
- `--clean`: Clean output directory first?

## Examples

### Deploy DAGs

```bash
starlake dag-deploy --outputDir /path/to/dags
```
