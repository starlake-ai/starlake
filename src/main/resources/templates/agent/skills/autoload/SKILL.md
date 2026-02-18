---
name: autoload
description: Automatically infer schemas and load data from the incoming directory
---

# AutoLoad Skill

Watches the incoming directory, automatically infers schemas for new data files, generates the corresponding YAML table definitions, and loads the data into the data warehouse. This is the quickest way to get data loaded — it combines schema inference and loading in a single step.

## Usage

```bash
starlake autoload [options]
```

## Options

- `--domains <value>`: Comma-separated list of domains to watch (default: all)
- `--tables <value>`: Comma-separated list of tables to watch (default: all)
- `--clean`: Overwrite existing mapping/schema files before starting
- `--accessToken <value>`: Access token for authentication (e.g. GCP)
- `--scheduledDate <value>`: Scheduled date for the job, format: `yyyy-MM-dd'T'HH:mm:ss.SSSZ`
- `--options k1=v1,k2=v2`: Substitution arguments passed to the watch job
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## How It Works

1. Scans the incoming directory for new data files
2. Infers the schema from file contents (column names, types)
3. Generates `_config.sl.yml` and `{table}.sl.yml` files in `metadata/load/`
4. Loads the data into the data warehouse using the inferred schema

The incoming directory is defined in `application.sl.yml` or `env.sl.yml`:

```yaml
# metadata/env.sl.yml
version: 1
env:
  incoming_path: "{{SL_ROOT}}/datasets/incoming"
```

## Configuration Context

AutoLoad creates table definitions like the following in `metadata/load/{domain}/`:

```yaml
# Auto-generated: metadata/load/starbake/_config.sl.yml
version: 1
load:
  name: "starbake"
  metadata:
    directory: "{{incoming_path}}/starbake"
```

```yaml
# Auto-generated: metadata/load/starbake/orders.sl.yml
version: 1
table:
  name: "orders"
  pattern: "orders_.*.json"
  attributes:
    - name: "customer_id"
      type: "long"
    - name: "order_id"
      type: "long"
    - name: "status"
      type: "string"
    - name: "timestamp"
      type: "iso_date_time"
  metadata:
    format: "JSON_FLAT"
    encoding: "UTF-8"
    array: true
    writeStrategy:
      type: "APPEND"
```

## Examples

### AutoLoad All Incoming Data

```bash
starlake autoload
```

### AutoLoad Specific Domain

```bash
starlake autoload --domains starbake
```

### AutoLoad Specific Tables

```bash
starlake autoload --domains starbake --tables orders,products
```

### AutoLoad with Clean (Re-infer Schemas)

Overwrite existing schema files and re-infer from data:

```bash
starlake autoload --clean
```

### AutoLoad with JSON Report

```bash
starlake autoload --reportFormat json
```

## Related Skills

- [load](../load/SKILL.md) - Load data with pre-defined schemas
- [infer-schema](../infer-schema/SKILL.md) - Infer schema for a single file
- [stage](../stage/SKILL.md) - Move files from landing to pending area
- [config](../config/SKILL.md) - Configuration reference