---
name: load
description: Load data from the pending area into the data warehouse
---

# Load Skill

Loads data files from the pending area into the data warehouse. Files must match the patterns defined in table configuration files (`table.sl.yml`). The load process validates data against the schema, applies write strategies, enforces data quality expectations, and applies privacy transformations.

## Usage

```bash
starlake load [options]
```

## Options

- `--domains <value>`: Comma-separated list of domains to load (default: all domains)
- `--tables <value>`: Comma-separated list of tables to load (default: all tables)
- `--accessToken <value>`: Access token for authentication (e.g. GCP)
- `--options k1=v1,k2=v2`: Substitution arguments passed to the load job
- `--test`: Run in test mode by running against the files tored in the metadata/tests directory without committing changes to the data warehouse
- `--files <value>`: Load only this specific file (fully qualified path)
- `--primaryKeys <value>`: Primary keys to set on the table schema (when inferring schema)
- `--scheduledDate <value>`: Scheduled date for the job, format: `yyyy-MM-dd'T'HH:mm:ss.SSSZ`
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

The `load` command relies on YAML configuration files in the `metadata/load/` directory.

### Domain Configuration (`metadata/load/{domain}/_config.sl.yml`)

Defines the domain name and shared metadata for all tables in the domain:

```yaml
# metadata/load/starbake/_config.sl.yml
version: 1
load:
  name: "starbake"
  metadata:
    directory: "{{incoming_path}}/starbake"
```

### Table Configuration (`metadata/load/{domain}/{table}.sl.yml`)

Defines the table schema, file pattern, format, write strategy, and attributes:

```yaml
# metadata/load/starbake/orders.sl.yml
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
    format: "JSON"
    encoding: "UTF-8"
    array: true
    writeStrategy:
      type: "APPEND"
```

### CSV Table Example

```yaml
# metadata/load/starbake/order_lines.sl.yml
version: 1
table:
  name: "order_lines"
  pattern: "order-lines_.*.csv"
  attributes:
    - name: "order_id"
      type: "int"
      sample: "99"
    - name: "product_id"
      type: "int"
      sample: "9"
    - name: "quantity"
      type: "int"
      sample: "5"
    - name: "sale_price"
      type: "double"
      sample: "8.68"
  metadata:
    format: "DSV"
    encoding: "UTF-8"
    withHeader: true
    separator: ";"
    writeStrategy:
      type: "APPEND"
```

### XML Table Example

```yaml
# metadata/load/starbake/products.sl.yml
version: 1
table:
  name: "products"
  pattern: "products.*.xml"
  attributes:
    - name: "category"
      type: "string"
    - name: "cost"
      type: "double"
    - name: "name"
      type: "string"
    - name: "price"
      type: "double"
    - name: "product_id"
      type: "long"
  metadata:
    format: "XML"
    encoding: "UTF-8"
    options:
      rowTag: "record"
    writeStrategy:
      type: "OVERWRITE"
```

### Write Strategies

The `writeStrategy.type` field controls how data is written:

| Strategy | Description |
|---|---|
| `APPEND` | Insert all rows |
| `OVERWRITE` | Replace entire table |
| `UPSERT_BY_KEY` | Update existing rows by key, insert new |
| `UPSERT_BY_KEY_AND_TIMESTAMP` | Upsert with timestamp comparison |
| `OVERWRITE_BY_PARTITION` | Replace specific partitions only |
| `DELETE_THEN_INSERT` | Delete matching keys then insert |
| `SCD2` | Slowly Changing Dimension Type 2 |

### Data Quality Expectations

```yaml
table:
  expectations:
    - expect: "is_col_value_not_unique('order_id') => result(0) == 1"
      failOnError: true
    - expect: "is_row_count_to_be_between(1, 1000000) => result(0) == 1"
      failOnError: false
```

## Examples

### Load All Domains

```bash
starlake load
```

### Load Specific Domain

```bash
starlake load --domains starbake
```

### Load Specific Table

```bash
starlake load --domains starbake --tables orders
```

### Load Multiple Tables

```bash
starlake load --domains starbake --tables orders,order_lines,products
```

### Test Load (Dry Run)

Run the load in test mode without writing to the warehouse:

```bash
starlake load --test
```

### Load a Specific File

```bash
starlake load --files /path/to/incoming/starbake/orders_20240301.json
```

### Load with Custom Options

```bash
starlake load --domains starbake --options date=2024-03-01,env=prod
```

### Load with JSON Report

```bash
starlake load --domains starbake --reportFormat json
```

## Related Skills

- [autoload](../autoload/SKILL.md) - Automatically infer schemas and load
- [stage](../stage/SKILL.md) - Move files from landing to pending area
- [preload](../preload/SKILL.md) - Check for files available for loading
- [infer-schema](../infer-schema/SKILL.md) - Infer schema from data files
- [transform](../transform/SKILL.md) - Run transformations on loaded data
- [config](../config/SKILL.md) - Configuration reference (write strategies, formats, types)