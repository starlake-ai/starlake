---
name: col-lineage
description: Generate column lineage
---

# Column Lineage Skill

This skill generates lineage at the column level for a specific task.

## Usage

```bash
starlake col-lineage [options]
```

## Options

- `--output <value>`: Output JSON file
- `--task <value>`: Task name (required)
- `--accessToken <value>`: Access token

## Examples

### Column Lineage for Task

```bash
starlake col-lineage --task mydomain.mytask
```
