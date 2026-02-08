---
name: table-dependencies
description: Generate table dependencies graph
---

# Table Dependencies Skill

This skill generates a graph showing dependencies between tables.

## Usage

```bash
starlake table-dependencies [options]
```

## Options

- `--output <value>`: Output file
- `--all-attrs`: Include all attributes
- `--reload`: Reload domains first
- `--svg`: Generate SVG
- `--png`: Generate PNG
- `--related`: Include only entities with relations
- `--tables <value>`: Include these tables
- `--all`: Include all tables
- `--json`: JSON output

## Examples

### Generate Table Graph

```bash
starlake table-dependencies --output tables.dot --svg --all
```
