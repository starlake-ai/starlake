---
name: cnxload
description: Load files (Parquet/CSV/JSON) into a JDBC table
---

# Connection Load Skill

Loads data from Parquet, CSV, or JSON files directly into a JDBC database table. This provides a simple way to push data into any JDBC-compatible database without going through the full Starlake load pipeline.

## Usage

```bash
starlake cnxload [options]
```

## Options

- `--source_file <value>`: Full path to the source file (required). Supports Parquet, CSV, JSON
- `--output_table <value>`: Target JDBC table in `schema.table` format (required)
- `--options k1=v1,k2=v2`: JDBC connection options:
  - `driver`: JDBC driver class (e.g. `org.postgresql.Driver`)
  - `user`: Database user
  - `password`: Database password
  - `url`: JDBC URL (e.g. `jdbc:postgresql://localhost:5432/mydb`)
  - `partitions`: Number of partitions for parallel write
  - `batchSize`: Batch size for inserts
- `--write_strategy <value>`: Write strategy: `APPEND`, `OVERWRITE`
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Load Parquet to PostgreSQL

```bash
starlake cnxload \
  --source_file /data/orders.parquet \
  --output_table public.orders \
  --options driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/mydb,user=admin,password=secret \
  --write_strategy APPEND
```

### Load CSV to Database

```bash
starlake cnxload \
  --source_file /data/customers.csv \
  --output_table public.customers \
  --options driver=org.postgresql.Driver,url=jdbc:postgresql://localhost:5432/mydb,user=admin,password=secret \
  --write_strategy OVERWRITE
```

### Load with Batch Options

```bash
starlake cnxload \
  --source_file /data/orders.parquet \
  --output_table sales.orders \
  --options driver=com.mysql.cj.jdbc.Driver,url=jdbc:mysql://localhost:3306/mydb,user=root,password=secret,batchSize=5000 \
  --write_strategy APPEND
```

## Related Skills

- [load](../load/SKILL.md) - Full Starlake load pipeline
- [esload](../esload/SKILL.md) - Load data into Elasticsearch
- [kafkaload](../kafkaload/SKILL.md) - Load/offload data to/from Kafka