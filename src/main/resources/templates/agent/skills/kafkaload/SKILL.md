---
name: kafkaload
description: Load or offload data to/from Kafka topics
---

# Kafka Load Skill

Loads data from files to Kafka topics or offloads data from Kafka topics to files. Supports both batch and streaming modes with optional transformations.

## Usage

```bash
starlake kafkaload [options]
```

## Options

### Read Options
- `--config <value>`: Kafka topic name (from connection configuration)
- `--connectionRef <value>`: Connection reference for the Kafka cluster
- `--format <value>`: Read format: `parquet`, `json`, `csv`, etc. (default: `parquet`)
- `--path <value>`: Source file path (for loading to Kafka) or target path (for offloading)
- `--options k1=v1,k2=v2`: Spark Reader options

### Write Options
- `--write-config <value>`: Write topic name
- `--write-path <value>`: Target file path for offloading
- `--write-mode <value>`: Write mode for file output
- `--write-options k1=v1,k2=v2`: Spark Writer options
- `--write-format <value>`: Streaming output format: `kafka`, `console`, etc.
- `--write-coalesce <value>`: Number of output partitions

### Processing Options
- `--transform <value>`: SQL transformation to apply to messages before loading/offloading
- `--stream`: Enable streaming mode (continuous processing)
- `--streaming-trigger <value>`: Trigger type: `Once`, `Continuous`, `ProcessingTime`
- `--streaming-trigger-option <value>`: Trigger interval (e.g. `10 seconds`)
- `--streaming-to-table <value>`: Sink to a table instead of files
- `--streaming-partition-by <value>`: Partition output by these columns
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Offload Kafka Topic to Parquet

```bash
starlake kafkaload --config orders_topic --path /data/output/orders --format parquet
```

### Load Files to Kafka Topic

```bash
starlake kafkaload --config orders_topic --path /data/input/orders.json --format json --write-config orders_output
```

### Stream from Kafka to Console

```bash
starlake kafkaload --config orders_topic --stream --write-format console
```

### Stream with Processing Time Trigger

```bash
starlake kafkaload --config orders_topic --stream --streaming-trigger ProcessingTime --streaming-trigger-option "10 seconds"
```

### Offload with Transformation

Apply a SQL transformation before saving:

```bash
starlake kafkaload --config orders_topic --path /data/output --transform "SELECT order_id, total FROM SL_THIS WHERE total > 100"
```

## Related Skills

- [load](../load/SKILL.md) - Load data from files
- [esload](../esload/SKILL.md) - Load data into Elasticsearch
- [cnxload](../cnxload/SKILL.md) - Load data into JDBC tables