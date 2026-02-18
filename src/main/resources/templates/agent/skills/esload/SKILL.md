---
name: esload
description: Load data into Elasticsearch
---

# ESLoad Skill

Loads data from files (Parquet, JSON) into Elasticsearch indices. Supports custom mappings, document IDs, and timestamp-based index naming.

## Usage

```bash
starlake esload [options]
```

## Options

- `--domain <value>`: Domain name (required)
- `--schema <value>`: Schema/table name (required)
- `--format <value>`: Input file format: `parquet`, `json`, or `json-array` (required)
- `--dataset <value>`: Path to the input dataset
- `--timestamp <value>`: Elasticsearch index timestamp suffix, e.g. `{@timestamp|yyyy.MM.dd}`
- `--id <value>`: Attribute name to use as the Elasticsearch document ID
- `--mapping <value>`: Path to a custom Elasticsearch mapping file
- `--conf k1=v1,k2=v2`: Elasticsearch-Spark configuration options
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Load Parquet Data to Elasticsearch

```bash
starlake esload --domain starbake --schema orders --format parquet
```

### Load JSON with Custom Document ID

```bash
starlake esload --domain starbake --schema orders --format json --id order_id
```

### Load with Timestamp-Based Index

```bash
starlake esload --domain starbake --schema orders --format parquet --timestamp "{@timestamp|yyyy.MM.dd}"
```

### Load with Custom Mapping

```bash
starlake esload --domain starbake --schema orders --format json --mapping /path/to/mapping.json
```

### Load from Specific Dataset Path

```bash
starlake esload --domain starbake --schema orders --format parquet --dataset /data/orders/
```

## Related Skills

- [index](../index/SKILL.md) - Alias for esload
- [load](../load/SKILL.md) - Load data into data warehouse
- [cnxload](../cnxload/SKILL.md) - Load data into JDBC tables