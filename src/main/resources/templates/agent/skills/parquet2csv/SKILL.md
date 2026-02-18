---
name: parquet2csv
description: Convert Parquet files to CSV format
---

# Parquet to CSV Skill

Converts Parquet files to CSV format. Useful for exporting data to systems that don't support Parquet, or for human-readable data inspection.

## Usage

```bash
starlake parquet2csv [options]
```

## Options

- `--input_dir <value>`: Full path to the input directory containing Parquet files (required)
- `--output_dir <value>`: Full path to the output directory for CSV files (default: same as `input_dir`)
- `--domain <value>`: Domain name to convert (filter by domain)
- `--schema <value>`: Schema/table name to convert (filter by table)
- `--delete_source`: Delete source Parquet files after successful conversion
- `--write_mode <value>`: Write mode: `OVERWRITE`, `APPEND`, `ERROR_IF_EXISTS`
- `--partitions <value>`: Number of output CSV file partitions
- `--options k1=v1,k2=v2`: Spark CSV writer options:
  - `sep` / `delimiter`: Field separator (default: `,`)
  - `quote`: Quote character
  - `quoteAll`: Quote all fields
  - `escape`: Escape character
  - `header`: Include header row (default: `true`)
  - `dateFormat`: Date format pattern
  - `timestampFormat`: Timestamp format pattern
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Convert All Parquet Files

```bash
starlake parquet2csv --input_dir /data/parquet --output_dir /data/csv
```

### Convert Specific Domain

```bash
starlake parquet2csv --input_dir /data/parquet --output_dir /data/csv --domain starbake
```

### Convert Specific Table

```bash
starlake parquet2csv --input_dir /data/parquet --output_dir /data/csv --domain starbake --schema orders
```

### Convert with Custom Separator

```bash
starlake parquet2csv --input_dir /data/parquet --output_dir /data/csv --options sep=;,header=true
```

### Convert and Delete Source

```bash
starlake parquet2csv --input_dir /data/parquet --output_dir /data/csv --delete_source
```

### Convert with Single Output Partition

```bash
starlake parquet2csv --input_dir /data/parquet --output_dir /data/csv --partitions 1
```

## Related Skills

- [load](../load/SKILL.md) - Load data (produces Parquet output)
- [extract-data](../extract-data/SKILL.md) - Extract data from databases