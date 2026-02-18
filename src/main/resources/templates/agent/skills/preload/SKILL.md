---
name: preload
description: Check for files available for loading in the landing/pending area
---

# PreLoad Skill

Checks for files available for loading in the landing or pending area. This is useful for orchestration to determine whether files are ready before triggering the actual load process.

## Usage

```bash
starlake preload [options]
```

## Options

- `--domain <value>`: Domain to check for files (required)
- `--tables <value>`: Comma-separated list of tables to check (default: all in domain)
- `--strategy <value>`: Pre-load strategy: `Imported`, `Pending`, or `Ack`
- `--globalAckFilePath <value>`: Path to the global acknowledgment file
- `--options k1=v1,k2=v2`: Substitution arguments
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Strategies

| Strategy   | Description |
|------------|---|
| `imported` | Check if files have been imported into the landing area |
| `pending`  | Check if files are available in the pending area |
| `ack`      | Check if an acknowledgment file exists before processing |

## Examples

### Check for Pending Files

```bash
starlake preload --domain starbake
```

### Check Specific Tables

```bash
starlake preload --domain starbake --tables orders,products
```

### Check with ACK Strategy

```bash
starlake preload --domain starbake --strategy ack --globalAckFilePath /data/pending/starbake/GO.ack
```

### Check with JSON Report

```bash
starlake preload --domain starbake --reportFormat json
```

## Related Skills

- [stage](../stage/SKILL.md) - Move files from landing to pending
- [load](../load/SKILL.md) - Load files into the data warehouse
- [dag-generate](../dag-generate/SKILL.md) - Generate DAGs with pre-load sensors