---
name: infer-schema
description: Infer a Starlake schema from a data file
---

# Infer Schema Skill

Analyzes a data file (CSV, JSON, XML, Parquet) and infers the Starlake table schema, generating the corresponding YAML configuration file with detected column names, types, and format metadata.

## Usage

```bash
starlake infer-schema [options]
```

## Options

- `--input <value>`: Path to the data file or directory to analyze (required)
- `--domain <value>`: Domain name for the generated schema (e.g. `starbake`)
- `--table <value>`: Table name for the generated schema (e.g. `orders`)
- `--outputDir <value>`: Output directory for the YAML file (default: `metadata/load`)
- `--write <value>`: Write mode: `OVERWRITE` or `APPEND`
- `--format <value>`: Force input file format: `DSV`, `JSON`, `JSON_FLAT`, `JSON_ARRAY`, `XML`, `PARQUET`
- `--rowTag <value>`: Row tag for XML files (e.g. `record`)
- `--variant`: Infer schema as a single variant attribute (schema-on-read)
- `--clean`: Delete previous YAML file before writing
- `--encoding <value>`: Input file encoding (default: `UTF-8`)
- `--from-json-schema`: Input file is a JSON Schema file (not data)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Generated Output

The command generates a table YAML file like:

```yaml
# Generated: metadata/load/starbake/orders.sl.yml
version: 1
table:
  name: "orders"
  pattern: "orders_.*.json"
  attributes:
    - name: "customer_id"
      type: "long"
      sample: "9"
    - name: "order_id"
      type: "long"
      sample: "99"
    - name: "status"
      type: "string"
      sample: "Pending"
    - name: "timestamp"
      type: "iso_date_time"
      sample: "2024-03-01T09:01:12.529Z"
  metadata:
    format: "JSON_FLAT"
    encoding: "UTF-8"
    array: true
    writeStrategy:
      type: "APPEND"
```

## Examples

### Infer from CSV File

```bash
starlake infer-schema --domain starbake --table order_lines --input /data/order-lines_20240301.csv --format DSV
```

### Infer from JSON File

```bash
starlake infer-schema --domain starbake --table orders --input /data/orders_20240301.json
```

### Infer from XML File

```bash
starlake infer-schema --domain starbake --table products --input /data/products.xml --rowTag record
```

### Infer from Parquet File

```bash
starlake infer-schema --domain starbake --table events --input /data/events.parquet
```

### Infer from JSON Schema

```bash
starlake infer-schema --domain starbake --table orders --input /schemas/orders.json --from-json-schema
```

### Overwrite Existing Schema

```bash
starlake infer-schema --domain starbake --table orders --input /data/orders.json --clean
```

### Infer as Variant (Schema-on-Read)

```bash
starlake infer-schema --domain starbake --table events --input /data/events.json --variant
```

## Related Skills

- [autoload](../autoload/SKILL.md) - Infer schemas and load in one step
- [load](../load/SKILL.md) - Load data using inferred schemas
- [xls2yml](../xls2yml/SKILL.md) - Generate schemas from Excel definitions
- [extract-schema](../extract-schema/SKILL.md) - Extract schema from a database