---
name: kafkaload
description: Load/Offload data to/from Kafka
---

# Kafka Load Skill

This skill handles data transfer between Kafka and files (loading and offloading).

## Usage

```bash
starlake kafkaload [options]
```

## Options

- `--config <value>`: Topic Name declared in reference.conf
- `--connectionRef <value>`: Connection reference
- `--format <value>`: Read/Write format (parquet, json, csv...)
- `--path <value>`: Source/Target path
- `--options <value>`: Spark Reader options
- `--write-config <value>`: Write Topic Name
- `--write-path <value>`: Write path
- `--write-mode <value>`: Write mode
- `--write-options <value>`: Spark Writer options
- `--write-format <value>`: Streaming format
- `--write-coalesce <value>`: Coalesce resulting dataframe
- `--transform <value>`: Transformation to apply
- `--stream`: Use streaming mode

## Examples

### Offload Topic to Parquet

```bash
starlake kafkaload --config mytopic --path /path/to/output
```
