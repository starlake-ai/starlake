---
name: summarize
description: Display table summary
---

# Summarize Skill

This skill runs a query to summarize a table's content (using stats).

## Usage

```bash
starlake summarize [options]
```

## Options

- `--domain <value>`: Domain Name (required)
- `--table <value>`: Table Name (required)
- `--accessToken <value>`: Access token

## Examples

### Summarize Table

```bash
starlake summarize --domain mydomain --table mytable
```
