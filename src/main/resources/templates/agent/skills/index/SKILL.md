---
name: index
description: Index data in Elasticsearch (alias for esload)
---

# Index Skill

Alias for the [esload](../esload/SKILL.md) command. All options and behavior are identical.

## Usage

```bash
starlake index [options]
```

## Options

See [esload](../esload/SKILL.md) for all available options.

## Examples

### Index Data in Elasticsearch

```bash
starlake index --domain starbake --schema orders --format parquet
```

## Related Skills

- [esload](../esload/SKILL.md) - Full documentation for this command