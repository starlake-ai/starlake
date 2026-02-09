---
name: lineage
description: Generate task dependencies graph (lineage)
---

# Lineage Skill

This skill generates a graph of task dependencies (data lineage).

## Usage

```bash
starlake lineage [options]
```

## Options

- `--output <value>`: Output file
- `--task <value>`: Compute dependencies of these tasks only
- `--reload`: Reload domains first
- `--viz`: Generate dot file
- `--svg`: Generate SVG
- `--json`: Generate JSON
- `--png`: Generate PNG
- `--print`: Print dependencies as text
- `--objects <value>`: Objects to display (task, table, view...)
- `--all`: Include all tasks
- `--verbose`: Add extra table properties

## Examples

### Generate Lineage

```bash
starlake lineage --output lineage.dot --svg --all
```
