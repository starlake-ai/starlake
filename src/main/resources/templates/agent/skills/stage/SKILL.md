---
name: stage
description: Move files from the landing area to the pending area
---

# Stage Skill

Moves data files from the landing area to the pending area, applying decompression and acknowledgment (ACK) file handling along the way. This is typically the first step in the ingestion pipeline before the `load` command processes the files.

## Usage

```bash
starlake stage [options]
```

## Options

- `--domains <value>`: Comma-separated list of domains to stage (default: all)
- `--tables <value>`: Comma-separated list of tables to stage (default: all)
- `--options k1=v1,k2=v2`: Substitution arguments for the stage process
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## How It Works

1. Scans the landing directory for new files matching domain/table patterns
2. Handles compressed files (gzip, zip) by decompressing them
3. Checks for acknowledgment (ACK) files if configured
4. Moves valid files to the pending area for processing by `load`

### Directory Flow

```
landing/           →  pending/           →  (load processes)
  └── starbake/         └── starbake/
      ├── orders.json       ├── orders.json
      └── orders.ack        └── (ack consumed)
```

### ACK File Configuration

Configure acknowledgment files in the domain `_config.sl.yml`:

```yaml
# metadata/load/starbake/_config.sl.yml
version: 1
load:
  name: "starbake"
  metadata:
    directory: "{{incoming_path}}/starbake" # this is the landing area
    ack: "ack"  # Require .ack file before processing if defined
```

## Examples

### Stage All Domains

```bash
starlake stage
```

### Stage Specific Domain

```bash
starlake stage --domains starbake
```

### Stage Specific Tables

```bash
starlake stage --domains starbake --tables orders,products
```

### Stage with JSON Report

```bash
starlake stage --reportFormat json
```

## Related Skills

- [preload](../preload/SKILL.md) - Check for files available for loading
- [load](../load/SKILL.md) - Load staged files into the data warehouse
- [autoload](../autoload/SKILL.md) - Automatically infer schemas and load
- [config](../config/SKILL.md) - Configuration reference (ACK, area settings)