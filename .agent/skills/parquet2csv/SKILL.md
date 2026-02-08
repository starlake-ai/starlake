---
name: parquet2csv
description: Convert parquet files to CSV
---

# Parquet 2 CSV Skill

This skill converts Parquet files to CSV format.

## Usage

```bash
starlake parquet2csv [options]
```

## Options

- `--input_dir <value>`: Full Path to input directory (required)
- `--output_dir <value>`: Full Path to output directory
- `--domain <value>`: Domain name to convert
- `--schema <value>`: Schema name to convert
- `--delete_source`: Delete source parquet files after conversion?
- `--write_mode <value>`: Write mode (OVERWRITE, APPEND, etc.)
- `--partitions <value>`: How many output partitions
- `--options <value>`: Spark options (sep, delimiter, header, etc.)

## Examples

### Convert Domain

```bash
starlake parquet2csv --input_dir /path/to/parquet --output_dir /path/to/csv --domain mydomain
```
