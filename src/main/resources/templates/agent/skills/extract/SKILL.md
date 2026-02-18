---
name: extract
description: Run both extract-schema and extract-data
---

# Extract Skill

This skill runs both schema extraction and data extraction in sequence.

## Usage

```bash
starlake extract [options]
```

This command allows you to extract both the schema (metadata) and the data from a JDBC source. It combines the functionality of `extract-schema` and `extract-data` into a single command.

## Options

See options for [extract-schema](../extract-schema/SKILL.md) and [extract-data](../extract-data/SKILL.md).

## Examples

### Extract Schema and Data

Extract schema and data for a specific table pattern from a JDBC source.

```bash
starlake extract --extract-config /path/to/extract-config.yml --output-dir /path/to/output
```

## Related Skills

- [extract-schema](../extract-schema/SKILL.md)
- [extract-data](../extract-data/SKILL.md)
