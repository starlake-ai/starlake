---
name: acl-dependencies
description: Generate ACL dependencies graph
---

# ACL Dependencies Skill

This skill generates a graph showing ACL dependencies.

## Usage

```bash
starlake acl-dependencies [options]
```

## Options

- `--output <value>`: Output file
- `--grantees <value>`: Include these users (all by default)
- `--reload`: Reload domains first
- `--svg`: Generate SVG
- `--json`: Generate JSON
- `--png`: Generate PNG
- `--tables <value>`: Include these tables
- `--all`: Include all ACLs

## Examples

### Generate ACL Graph

```bash
starlake acl-dependencies --output acl_graph.dot --svg
```
