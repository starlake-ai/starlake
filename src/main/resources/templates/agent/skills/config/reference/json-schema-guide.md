# Starlake JSON Schema Complete Reference

> **Schema URL**: https://www.schemastore.org/starlake.json
>
> **Schema ID**: `https://json.schemastore.org/starlake.json`
>
> **Draft Version**: JSON Schema Draft-07

## Schema Overview

This schema defines the complete structure for Starlake, a data pipeline framework supporting:
- **Extract**: Pull data from JDBC databases and OpenAPI endpoints
- **Load**: Ingest files (CSV, JSON, Parquet, XML) into data warehouses
- **Transform**: SQL and Python-based analytics workflows

## Root Structure

Every Starlake configuration file must include:
```yaml
version: 1  # Required integer enum
```

And at least one of these top-level keys:
- `extract`: Data extraction configuration
- `load`: Data ingestion schemas
- `transform`: Analytical job definitions
- `env`: Environment variables
- `types`: Custom type definitions
- `table`: Standalone table schema
- `task`: Standalone task definition
- `application`: Global application settings
- `refs`: External references
- `dag`: Workflow orchestration

## Core Configuration Objects

### AppConfigV1 (Application Configuration)

Global system settings and defaults.

**Key Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Application identifier |
| `connectionRef` | string | Default connection name (supports templating) |
| `connections` | object | Map of connection name → ConnectionV1 |
| `env` | string | Environment folder path |
| `datasets` | string | Datasets folder path |
| `incoming` | string | Incoming data folder path |
| `metadata` | string | Metadata folder path |
| `loader` | enum | Processing engine: `spark` or `native` |
| `grouped` | boolean | Group files before processing |
| `parallelism` | integer | Max parallel tasks |
| `expectations` | object | Data quality settings |
| `metrics` | MetricsV1 | Metrics configuration |
| `audit` | AuditV1 | Audit settings |
| `archive` | boolean | Archive processed files |
| `timezone` | string | Timezone for scheduling (e.g., "UTC") |

**Example:**
```yaml
version: 1
application:
  name: "data-platform"
  connectionRef: "{{defaultDb}}"
  connections:
    postgres:
      type: jdbc
      url: "jdbc:postgresql://{{PG_HOST}}:{{PG_PORT}}/{{PG_DB}}"
  loader: spark
  parallelism: 4
  expectations:
    active: true
  metrics:
    active: true
```

### ConnectionV1 (Database Connection)

Defines how to connect to data sources and targets.

**Key Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `type` | enum | Connection type (see below) |
| `sparkFormat` | string | Spark data source format |
| `quote` | string | Identifier quoting character (default: `"`) |
| `separator` | string | Catalog/schema separator (default: `.`) |
| `options` | object | Connection-specific key-value options |

**Connection Types:**
- `jdbc`: Generic JDBC
- `bigquery`: Google BigQuery
- `snowflake`: Snowflake
- `redshift`: Amazon Redshift
- `databricks`: Databricks SQL
- `postgres`: PostgreSQL
- `mysql`: MySQL
- `duckdb`: DuckDB
- `parquet`: Parquet files
- `kafka`: Apache Kafka

**Example:**
```yaml
connections:
  duckdb:
    type: duckdb
    url: "jdbc:duckdb:{{sl_root}}/datasets/duckdb.db"
    options:
      s3_endpoint: "s3.amazonaws.com"
      s3_access_key_id: "{{AWS_KEY}}"
      s3_secret_access_key: "{{AWS_SECRET}}"
```

### DomainV1 (Load Configuration)

Organizes ingested data into logical domains.

**Key Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Domain name (becomes folder/dataset) |
| `database` | string | Target database reference |
| `tables` | array | List of TableV1 schemas |
| `metadata` | object | Default parsing metadata for all tables |
| `comment` | string | Documentation |
| `tags` | array | Classification tags |

**Example:**
```yaml
version: 1
load:
  name: "sales"
  database: "analytics"
  metadata:
    format: DSV
    separator: ","
    withHeader: true
  tables:
    - name: "transactions"
      pattern: "sales_*.csv"
```

### TableV1 (Table Schema)

Defines data structure and parsing rules.

**Key Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Table name |
| `pattern` | string | Filename pattern (glob/regex) |
| `attributes` | array | List of AttributeV1 (columns) |
| `metadata` | object | Format-specific parsing rules |
| `writeStrategy` | WriteStrategyV1 | How to persist data |
| `expectations` | array | SQL data quality checks |
| `freshness` | FreshnessV1 | Staleness thresholds |
| `rls` | array | Row-level security policies |
| `acl` | array | Column-level access control |
| `partition` | object | Partitioning configuration |
| `comment` | string | Documentation |
| `tags` | array | Classification tags |

**Metadata Formats:**

```yaml
# CSV/TSV (DSV)
metadata:
  format: DSV
  separator: ","
  withHeader: true
  quote: "\""
  escape: "\\"
  encoding: "UTF-8"

# JSON
metadata:
  format: JSON
  array: false  # true for JSON_ARRAY

# Parquet
metadata:
  format: PARQUET

# Fixed-width (POSITION)
metadata:
  format: POSITION
  # Positions defined in attribute.position

# XML
metadata:
  format: XML
  xpath: "/root/record"
```

**Example:**
```yaml
tables:
  - name: "orders"
    pattern: "order_*.csv"
    metadata:
      format: DSV
      separator: ","
      withHeader: true
      mode: FILE
      directory: "{{SL_ROOT}}/incoming/orders/"
    writeStrategy:
      type: UPSERT_BY_KEY
      key: ["order_id"]
    attributes:
      - name: order_id
        type: string
        required: true
      - name: amount
        type: decimal
        required: true
```

### AttributeV1 (Column Definition)

Individual column schema and transformations.

**Key Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Column name |
| `type` | string | Primitive type or custom type |
| `required` | boolean | NOT NULL constraint |
| `array` | boolean | Array/list column |
| `privacy` | enum | Privacy transformation (see below) |
| `metricType` | enum | `DISCRETE`, `CONTINUOUS`, `TEXT`, `NONE` |
| `comment` | string | Documentation |
| `rename` | string | Original column name (for renaming) |
| `script` | string | SQL transformation (applied to renamed column) |
| `foreignKey` | string | Reference to `[domain.]table[.attribute]` |
| `position` | object | Fixed-width position (`first`, `last` character) |
| `tags` | array | Classification tags |

**Privacy Transformations:**
- `none`: No transformation
- `hide`: Completely hide value
- `hideX(char, count)`: Partial masking (e.g., `hide4(*, 4)`)
- `md5`: MD5 hash
- `sha1`: SHA-1 hash
- `sha256`: SHA-256 hash
- `sha512`: SHA-512 hash
- `initials`: First letter only

**Primitive Types:**
- **Numeric**: `string`, `long`, `int`, `short`, `double`, `decimal`, `byte`
- **Temporal**: `date` (yyyy-MM-dd), `timestamp` (yyyy-MM-dd HH:mm:ss)
- **Structured**: `struct`, `variant` (JSON/XML)
- **Other**: `boolean`

**Example:**
```yaml
attributes:
  - name: customer_id
    type: string
    required: true
    foreignKey: "customers.customer_id"

  - name: email
    type: string
    privacy: sha256  # Hash for privacy

  - name: credit_card
    type: string
    privacy: hide    # Completely hide

  - name: amount
    type: decimal
    required: true
    metricType: CONTINUOUS
```

### WriteStrategyV1 (Persistence Strategy)

How data is written to the target.

**Strategy Types:**

| Type | Description | Required Properties |
|------|-------------|---------------------|
| `APPEND` | Add new records | - |
| `OVERWRITE` | Replace all data | - |
| `UPSERT_BY_KEY` | Update or insert by key | `key` |
| `UPSERT_BY_KEY_AND_TIMESTAMP` | Key + timestamp merge | `key`, `timestamp` |
| `DELETE_THEN_INSERT` | Clear then insert | - |
| `OVERWRITE_BY_PARTITION` | Partition-level replace | - |
| `SCD2` | Slowly Changing Dimension Type 2 | `key`, `timestamp`, `startTs`, `endTs` |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `type` | enum | Strategy type |
| `key` | array | Key columns for matching records |
| `timestamp` | string | Timestamp column for comparison |
| `on` | enum | `TARGET` or `SOURCE_AND_TARGET` |
| `startTs` | string | SCD2 validity start timestamp |
| `endTs` | string | SCD2 validity end timestamp |

**Examples:**

```yaml
# Simple append
writeStrategy:
  type: APPEND

# Upsert by key
writeStrategy:
  type: UPSERT_BY_KEY
  key: ["customer_id"]

# Upsert with timestamp
writeStrategy:
  type: UPSERT_BY_KEY_AND_TIMESTAMP
  key: ["order_id"]
  timestamp: "updated_at"

# SCD Type 2
writeStrategy:
  type: SCD2
  key: ["product_id"]
  timestamp: "effective_date"
  startTs: "valid_from"
  endTs: "valid_to"

# Partition overwrite
writeStrategy:
  type: OVERWRITE_BY_PARTITION
```

### AutoJobDescV1 (Transform Configuration)

SQL and Python-based analytical workflows.

**Key Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Job name |
| `tasks` | array | List of AutoTaskDescV1 |
| `default` | object | Default properties for all tasks |
| `comment` | string | Documentation |
| `tags` | array | Classification tags |

**Example:**
```yaml
version: 1
transform:
  name: "sales_analytics"
  default:
    connectionRef: "postgres"
    writeStrategy:
      type: OVERWRITE
  tasks:
    - name: "daily_sales"
      sql: |
        SELECT
          DATE(order_date) as date,
          SUM(amount) as total_sales
        FROM sales.orders
        GROUP BY DATE(order_date)
      domain: "analytics"
      table: "daily_sales"
```

### AutoTaskDescV1 (Task Definition)

Individual transformation task.

**Key Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `name` | string | Task name |
| `sql` | string | SQL query (mutually exclusive with `python`) |
| `python` | string | Python script path |
| `domain` | string | Target domain |
| `table` | string | Target table |
| `connectionRef` | string | Database connection override |
| `writeStrategy` | WriteStrategyV1 | Persistence strategy |
| `partition` | object | Partitioning config |
| `presql` | array | SQL to run before task |
| `postsql` | array | SQL to run after task |
| `expectations` | array | Data quality checks |
| `syncStrategy` | enum | Schema sync: `NONE`, `ADD`, `ALL` |

**Example:**
```yaml
tasks:
  - name: "customer_ltv"
    sql: |
      SELECT
        customer_id,
        SUM(amount) as lifetime_value,
        COUNT(*) as order_count
      FROM sales.orders
      GROUP BY customer_id
    domain: "analytics"
    table: "customer_ltv"
    partition:
      sampling: 1.0
      attributes: ["customer_id"]
    writeStrategy:
      type: OVERWRITE
    expectations:
      - "SELECT COUNT(*) FROM {{table}} WHERE lifetime_value > 0"
```

## Advanced Features

### Row-Level Security (RLS)

Filter data access per user/group.

```yaml
rls:
  - name: "region_filter"
    predicate: "region = '{{user_region}}'"
    grants:
      - "user:john@example.com"
      - "group:sales-team@example.com"
```

### Column-Level Access Control (ACL)

Restrict column visibility.

```yaml
acl:
  - role: "analyst"
    grants:
      - "user:jane@example.com"
      - "group:analytics@example.com"
    name: "sensitive_columns"
```

### Data Quality Expectations

SQL-based validation.

```yaml
expectations:
  - "SELECT COUNT(*) FROM {{table}} WHERE id IS NOT NULL"
  - "SELECT COUNT(*) FROM {{table}} WHERE amount >= 0"
  - "SELECT COUNT(*) FROM {{table}} WHERE email LIKE '%@%'"
```

### Metrics Configuration

```yaml
metrics:
  active: true
  discreteMaxCardinality: 10  # Max unique values for discrete metrics
  path: "{{SL_ROOT}}/metrics"
```

### Freshness Monitoring

```yaml
freshness:
  warn: "1 day"   # Warn if data older than 1 day
  error: "3 day"  # Error if data older than 3 days
```

### Partitioning

```yaml
partition:
  sampling: 1.0  # Fraction of data to sample
  attributes: ["year", "month", "day"]
```

### Custom Types

Define reusable types with regex validation.

```yaml
version: 1
types:
  - name: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    primitiveType: string
    comment: "Valid email address"

  - name: "iso_date"
    pattern: "^\\d{4}-\\d{2}-\\d{2}$"
    primitiveType: date
    comment: "ISO 8601 date (YYYY-MM-DD)"
```

## Extract Configurations

### JDBC Extract

Pull data from databases.

```yaml
version: 1
extract:
  jdbcSchemas:
    - catalog: "production"
      schema: "sales"
      connection: "postgres"
      tableTypes: ["TABLE", "VIEW"]
      tables:
        - name: "orders"
          partitionColumn: "order_date"
          numPartitions: 4
          filter: "order_date >= CURRENT_DATE - INTERVAL '30 days'"
```

### OpenAPI Extract

Extract from REST APIs.

```yaml
version: 1
extract:
  openapis:
    - url: "https://api.example.com/openapi.json"
      domains:
        - name: "customers"
          routes:
            - path: "/customers"
              operations: ["GET"]
```

## Variable Substitution

Starlake supports two types of variable substitution:

### 1. Template Variables (Runtime)
Use `{{variable}}` syntax:
```yaml
url: "jdbc:postgresql://{{PG_HOST}}:{{PG_PORT}}/{{PG_DB}}"
```

### 2. Environment Variables (Parse Time)
Use `${ENV_VAR}` syntax in `env.sl.yml`:
```yaml
env:
  PG_HOST: "${POSTGRES_HOST}"
  PG_PORT: "${POSTGRES_PORT}"
```

## Schema Validation

### VS Code Integration

Add to `.vscode/settings.json`:
```json
{
  "yaml.schemas": {
    "https://json.schemastore.org/starlake.json": [
      "metadata/**/*.sl.yml",
      "**/metadata/**/*.sl.yml"
    ]
  }
}
```

### CLI Validation

```bash
starlake validate --config metadata/application.sl.yml
```

## Complete Example

### Project Structure
```
my-project/
└── metadata/
    ├── application.sl.yml
    ├── env.sl.yml
    ├── types/
    │   └── types.sl.yml
    ├── load/
    │   └── sales.sl.yml
    └── transform/
        └── analytics.sl.yml
```

### application.sl.yml
```yaml
version: 1
application:
  name: "sales-analytics"
  connectionRef: "{{activeConnection}}"
  connections:
    duckdb:
      type: duckdb
      url: "jdbc:duckdb:{{sl_root}}/datasets/duckdb.db"
  loader: native
  expectations:
    active: true
  metrics:
    active: true
    path: "{{SL_ROOT}}/metrics"
```

### env.sl.yml
```yaml
version: 1
env:
  sl_root: "/projects/{{member_id}}/{{project_id}}"
  activeConnection: "duckdb"
  member_id: "${SL_MEMBER_ID}"
  project_id: "${SL_PROJECT_ID}"
```

### load/sales.sl.yml
```yaml
version: 1
load:
  name: "sales"
  tables:
    - name: "orders"
      pattern: "orders_*.csv"
      metadata:
        format: DSV
        separator: ","
        withHeader: true
        mode: FILE
        directory: "{{SL_ROOT}}/incoming/sales/"
      writeStrategy:
        type: UPSERT_BY_KEY
        key: ["order_id"]
      attributes:
        - name: order_id
          type: string
          required: true
        - name: amount
          type: decimal
          required: true
```

### transform/analytics.sl.yml
```yaml
version: 1
transform:
  name: "sales_metrics"
  tasks:
    - name: "daily_totals"
      sql: |
        SELECT
          DATE(order_date) as date,
          SUM(amount) as total
        FROM sales.orders
        GROUP BY DATE(order_date)
      domain: "analytics"
      table: "daily_sales"
      writeStrategy:
        type: OVERWRITE
```

## Resources

- **JSON Schema**: https://www.schemastore.org/starlake.json
- **Documentation**: https://docs.starlake.ai
- **GitHub**: https://github.com/starlake-ai/starlake
- **Examples**: https://github.com/starlake-ai/starlake-examples
