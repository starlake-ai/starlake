# Bidirectional Kafka-BigQuery Support in KafkaJob

**Date:** 2026-04-01
**Status:** Approved
**Approach:** Minimal extension of KafkaJob (Approach A)

## Overview

Extend `KafkaJob` to support BigQuery as both a source and a sink, treating `"bigquery"` as another Spark format alongside `"kafka"`, `"json"`, `"parquet"`, and `"org.elasticsearch.spark.sql"`.

Two new data paths:

```
Kafka Topic -> KafkaJob (batch/stream) -> DataFrame -> Spark BQ connector -> BigQuery table
BigQuery table/query -> Spark BQ connector -> DataFrame -> KafkaJob -> KafkaClient.sinkToTopic() -> Kafka Topic
```

No new classes. Changes confined to existing Kafka subsystem files.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Integration point | Extend KafkaJob directly | Self-contained in kafka subsystem, follows ES pattern |
| BQ -> Kafka trigger | SQL query + full table | Query is primary; full table is `SELECT *` |
| Incremental tracking (BQ->Kafka) | Full query each run | User manages incrementality in SQL; watermark tracking deferred |
| Kafka -> BQ write strategies | Append + Overwrite | Merge/Upsert deferred |
| Batch + Streaming | Both supported (sink side) | BQ as streaming source rejected with error |
| Connection config | Inline options in KafkaJobConfig | BQ options passed via `--write-options` / `--options` |
| Schema mapping | Auto-infer + explicit override | Spark BQ connector auto-infers; `fields` config for explicit |
| Testing | Real BigQuery | GCP credentials required in CI |

## Kafka -> BigQuery (Sink)

### Batch Mode

In `batchSave()`, when `writeFormat = "bigquery"`, the DataFrame writes via Spark's `format("bigquery")` with options from `writeOptions`. The existing flow already does this naturally:

```scala
df.write.mode(writeMode).format(writeFormat).options(writeOptions).save()
```

Additions:
- **Skip coalesce/single-file logic** when `writeFormat = "bigquery"` (file-specific, irrelevant for BQ)
- **`writePath` not used** for BigQuery; target table specified via `writeOptions("table")`
- **Required writeOptions**: `table` (e.g., `project:dataset.table`), `temporaryGcsBucket`
- **Optional writeOptions**: `project`, `parentProject`, `writeMethod` (direct/indirect), `credentials`

### Streaming Mode

In `writeStreaming()`, when `writeFormat = "bigquery"`, the streaming DataFrame writes to BigQuery. The Spark BQ connector supports `writeStream.format("bigquery")` natively. The existing non-kafka branch (line 238-243) handles this — it uses `writeFormat` and `writeOptions` directly.

### Write Modes

- `Append` -> `SaveMode.Append` (default)
- `Overwrite` -> `SaveMode.Overwrite`

Both already supported via `kafkaJobConfig.writeMode`.

### CLI Examples

```bash
# Batch: Kafka topic -> BigQuery table
starlake kafkaload \
  --config my_topic \
  --write-format bigquery \
  --write-mode Append \
  --write-options table=myproject:mydataset.mytable,temporaryGcsBucket=my-bucket

# Streaming: Kafka topic -> BigQuery table
starlake kafkaload \
  --config my_topic \
  --stream \
  --write-format bigquery \
  --write-options table=myproject:mydataset.mytable,temporaryGcsBucket=my-bucket
```

## BigQuery -> Kafka (Source)

When `format = "bigquery"`, `KafkaJob.pipeline()` reads from BigQuery instead of a file or Kafka topic. This uses the `case None` branch of `topicConfig` match (line 135), the same path used for file->Kafka loading.

### Table Read

Existing code at line 144-151:
```scala
session.read.format(kafkaJobConfig.format).load(finalLoadPath...)
```

For BigQuery, `format = "bigquery"` and `path` = fully qualified table reference (e.g., `myproject:mydataset.mytable`). The Spark BQ connector handles this natively.

### SQL Query Read

When `format = "bigquery"` and `options` contains a `query` key, use `.options(options).load()` instead of `.load(path)`. Small addition in `pipeline()`:

```scala
// When format is bigquery and query option is present, use options-based loading
val df = if (kafkaJobConfig.format == "bigquery" && options.contains("query")) {
  session.read.format("bigquery").options(options).load()
} else {
  session.read.format(kafkaJobConfig.format).load(finalLoadPath...)
}
```

### Streaming Source

Not supported. BigQuery is a batch source. When `format = "bigquery"` and `streaming = true`, reject with error: `"BigQuery cannot be used as a streaming source. Use batch mode."`

### CLI Examples

```bash
# Full table -> Kafka topic
starlake kafkaload \
  --format bigquery \
  --path "myproject:mydataset.mytable" \
  --options temporaryGcsBucket=my-bucket \
  --write-config my_output_topic

# SQL query -> Kafka topic
starlake kafkaload \
  --format bigquery \
  --options query="SELECT * FROM mydataset.mytable WHERE updated_at > '2024-01-01'",temporaryGcsBucket=my-bucket \
  --write-config my_output_topic
```

## Validation and Error Handling

### When `writeFormat = "bigquery"`:
- Require `writeOptions("table")` -> error: `"BigQuery sink requires 'table' in write-options (e.g., project:dataset.table)"`
- Require `writeOptions("temporaryGcsBucket")` -> error: `"BigQuery sink requires 'temporaryGcsBucket' in write-options"`
- Skip coalesce single-file renaming logic (guard with `if writeFormat != "bigquery"`)

### When `format = "bigquery"`:
- If neither `path` nor `options("query")` is set -> error: `"BigQuery source requires either --path (table reference) or --options query=... (SQL query)"`
- If `options("query")` is set but `options("temporaryGcsBucket")` is missing -> error: `"BigQuery query source requires 'temporaryGcsBucket' in options for query materialization"`
- If `streaming = true` -> error: `"BigQuery cannot be used as a streaming source. Use batch mode."`

### Authentication

Delegated entirely to the Spark BigQuery connector. No auth handling in KafkaJob. The connector respects:
- `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Application Default Credentials
- Explicit credentials via options (`credentials`, `gcpAccessToken`)

## Unit Tests

New test class `KafkaBigQueryJobSpec` alongside existing `KafkaJobSpec`. Uses embedded Kafka container + real BigQuery. Gated by `SL_KAFKA_BQ_TEST_DISABLE` environment variable.

### Test Cases

1. **Kafka -> BigQuery (batch append)**: Produce JSON messages to Kafka -> `KafkaJob` with `writeFormat = "bigquery"` -> verify rows in BQ
2. **Kafka -> BigQuery (batch overwrite)**: Same with `writeMode = Overwrite` -> verify table replaced
3. **Kafka -> BigQuery (streaming)**: Streaming mode with `streamingTrigger = "Once"` -> verify rows in BQ
4. **BigQuery -> Kafka (table read)**: Seed BQ table -> `KafkaJob` with `format = "bigquery"` -> verify messages in Kafka topic
5. **BigQuery -> Kafka (SQL query)**: Query with filter -> verify filtered results in Kafka
6. **Validation: missing table option**: Expect clear error
7. **Validation: BQ as streaming source**: Expect clear error
8. **Offset tracking**: Kafka -> BQ batch run twice -> verify offsets saved, no reprocessing

### Test Config

Extends existing `kafkaConfig()` pattern. BQ options from environment variables:
- `SL_BQ_TEST_PROJECT` - GCP project ID
- `SL_BQ_TEST_DATASET` - BigQuery dataset
- `SL_BQ_TEST_BUCKET` - GCS staging bucket

## Files Changed

| File | Change | Est. Lines |
|------|--------|------------|
| `KafkaJob.scala` | BQ validation in `pipeline()`, coalesce guard in `batchSave()`, query option handling | ~30 |
| `KafkaJobCmd.scala` | Update help text with BQ examples | ~15 |
| `KafkaBigQueryJobSpec.scala` | New test class, 8 test cases | ~250 |

### Files NOT Changed

- `KafkaClient.scala` - handles Kafka side only
- `KafkaJobConfig.scala` - existing fields cover all needs
- `BigQuerySparkJob.scala` / `BigQueryJobBase.scala` - no coupling
- `Settings.scala` / `KafkaTopicConfig` - no new config fields

### Dependencies

None new. Spark BigQuery connector already in the build (used by `BigQuerySparkJob`).