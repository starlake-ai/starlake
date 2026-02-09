---
name: esload
description: Load data into Elasticsearch
---

# ESLoad Skill

This skill loads data into Elasticsearch.

## Usage

```bash
starlake esload [options]
```

## Options

- `--domain <value>`: Domain Name (required)
- `--schema <value>`: Schema Name (required)
- `--format <value>`: Dataset input file format (parquet, json, json-array) (required)
- `--timestamp <value>`: Elasticsearch index timestamp suffix
- `--id <value>`: Elasticsearch Document Id attribute
- `--mapping <value>`: Path to Elasticsearch Mapping File
- `--dataset <value>`: Input dataset path
- `--conf <value>`: esSpark configuration options

## Examples

### Load Parquet to ES

```bash
starlake esload --domain mydomain --schema mytable --format parquet
```
