---
skill_name: config
version: 2.0.0
description: Comprehensive Starlake data pipeline configuration patterns, complete environment variables catalog, JSON Schema reference, and production-ready best practices
tags:
  [
    starlake,
    data-engineering,
    etl,
    yaml,
    configuration,
    schema,
    spark,
    duckdb,
    bigquery,
    snowflake,
    airflow,
    dagster,
  ]
author: Starlake Team
created: 2026-02-06
updated: 2026-02-06
---

# Starlake Configuration Skill (Complete Reference)

Expert knowledge for creating and validating Starlake data pipeline configurations using the official JSON Schema and production-tested patterns extracted from official documentation.

## Overview

Starlake uses YAML configuration files validated against a JSON Schema available at:

- **Schema URL**: https://www.schemastore.org/starlake.json
- **Schema ID**: `https://json.schemastore.org/starlake.json`
- **Draft Version**: JSON Schema Draft-07

## Core Configuration Files

### File Structure

```
metadata/
├── application.sl.yml           # Global app configuration, connections
├── env.sl.yml                   # Global environment variables
├── env.{ENV}.sl.yml            # Environment-specific overrides (PROD, DEV, etc.)
├── types/
│   ├── default.sl.yml          # Built-in type definitions
│   └── custom.sl.yml           # Custom type definitions
├── load/
│   └── {domain}/
│       ├── _config.sl.yml      # Domain-level configuration
│       └── {table}.sl.yml      # Table schemas
├── transform/
│   └── {domain}/
│       ├── {task}.sl.yml       # Task configuration
│       ├── {task}.sql          # SQL transform
│       └── {task}.py           # Python transform (optional)
├── extract/
│   └── {config}.sl.yml         # JDBC/API extraction configs
├── dags/
│   ├── {dag}.sl.yml            # DAG configuration
│   └── template/
│       └── {template}.py.j2    # Custom DAG templates
└── expectations/
    └── {name}.j2               # Jinja data quality macros
```

---

## Environment Variables Reference

### Core Variables

| Variable      | Purpose                            | Default             | Example                            |
| ------------- | ---------------------------------- | ------------------- | ---------------------------------- |
| `SL_ROOT`     | Root directory for project         | -                   | `/projects/100/101`                |
| `SL_ENV`      | Environment selector for env files | -                   | `DEV`, `PROD`, `DUCKDB`, `BQ`      |
| `SL_DATASETS` | Location of datasets directory     | `{{root}}/datasets` | `/projects/100/101/datasets`       |
| `SL_METADATA` | Metadata directory location        | `{{root}}/metadata` | `/projects/100/101/metadata`       |
| `SL_INCOMING` | Incoming files directory           | `{{root}}/incoming` | `/projects/100/101/incoming`       |
| `SL_ARCHIVE`  | Archive processed files            | `true`              | `true` / `false`                   |
| `SL_FS`       | Filesystem type                    | -                   | `file://`, `s3a://`, `hdfs://`     |
| `SL_TIMEZONE` | Timezone for date operations       | `UTC`               | `Europe/Paris`, `America/New_York` |

### Area-Specific Variables

| Variable                | Purpose                     | Default             |
| ----------------------- | --------------------------- | ------------------- |
| `SL_AREA_PENDING`       | Files pending processing    | `pending`           |
| `SL_AREA_UNRESOLVED`    | Files not matching patterns | `unresolved`        |
| `SL_AREA_ARCHIVE`       | Processed files archive     | `archive`           |
| `SL_AREA_INGESTING`     | Files being processed       | `ingesting`         |
| `SL_AREA_ACCEPTED`      | Valid records location      | `accepted`          |
| `SL_AREA_REJECTED`      | Invalid records location    | `rejected`          |
| `SL_AREA_BUSINESS`      | Transform results location  | `business`          |
| `SL_AREA_REPLAY`        | Rejected records replay     | `replay`            |
| `SL_AREA_HIVE_DATABASE` | Hive database name pattern  | `${domain}_${area}` |

### Component-Specific Variables

| Variable             | Purpose                         | Component      | Default                   |
| -------------------- | ------------------------------- | -------------- | ------------------------- |
| `SL_METRICS_ACTIVE`  | Enable metrics computation      | Load/Transform | `true`                    |
| `SL_HIVE`            | Store as Hive/Databricks tables | Spark          | `false`                   |
| `SL_AUDIT_SINK_TYPE` | Audit log destination           | Audit          | `BigQuerySink`, `FsSink`  |
| `SL_API_HTTP_PORT`   | API server port                 | API            | `11000`                   |
| `SL_API_DOMAIN`      | API server domain/IP            | API            | `localhost`               |
| `SL_UI_PORT`         | UI server port                  | UI             | `8080`                    |
| `SL_STARLAKE_PATH`   | Path to starlake executable     | Orchestrator   | `/usr/local/bin/starlake` |

### Predefined Template Variables (Auto-Generated)

| Variable          | Format                  | Example          | Usage              |
| ----------------- | ----------------------- | ---------------- | ------------------ |
| `sl_date`         | yyyyMMdd                | `20260206`       | Filename patterns  |
| `sl_datetime`     | yyyyMMddHHmmss          | `20260206143000` | Timestamp patterns |
| `sl_year`         | yyyy                    | `2026`           | Partitioning       |
| `sl_month`        | MM                      | `02`             | Partitioning       |
| `sl_day`          | dd                      | `06`             | Partitioning       |
| `sl_hour`         | HH                      | `14`             | Time partitions    |
| `sl_minute`       | mm                      | `30`             | Time partitions    |
| `sl_second`       | ss                      | `00`             | Time partitions    |
| `sl_milli`        | SSS                     | `123`            | Precision          |
| `sl_epoch_second` | Seconds since 1970      | `1738850400`     | Timestamps         |
| `sl_epoch_milli`  | Milliseconds since 1970 | `1738850400000`  | Timestamps         |

---

## Application Configuration

### Complete Application Structure

```yaml
# metadata/application.sl.yml
version: 1
application:
  name: "my-data-platform"
  connectionRef: "{{activeConnection}}"

  # Default write format
  defaultWriteFormat: parquet # parquet, delta, iceberg, json, csv

  # Load strategy class (how files are ordered for processing)
  loadStrategyClass: "ai.starlake.job.load.IngestionTimeStrategy"

  # SCD2 default column names
  scd2StartTimestamp: "sl_start_ts"
  scd2EndTimestamp: "sl_end_ts"

  # Timezone for date operations
  timezone: "UTC"

  # Storage paths
  datasets: "{{root}}/datasets"
  incoming: "{{root}}/incoming"
  metadata: "{{root}}/metadata"

  # Processing
  loader: native # or spark
  grouped: true
  parallelism: 4

  # Area configuration
  area:
    pending: "pending"
    unresolved: "unresolved"
    archive: "archive"
    ingesting: "ingesting"
    accepted: "accepted"
    rejected: "rejected"
    business: "business"
    replay: "replay"
    hiveDatabase: "${domain}_${area}"

  # Audit configuration
  audit:
    sink:
      connectionRef: "{{activeConnection}}"

  # Access policies (BigQuery Column-Level Security)
  accessPolicies:
    apply: true
    location: EU
    taxonomy: RGPD

  # Default DAG references for orchestration
  dagRef:
    load: "default_load_dag"
    transform: "default_transform_dag"

  # Connections (see Connection Types section)
  connections:
    duckdb-local:
      type: jdbc
      options:
        url: "jdbc:duckdb:{{sl_root_local}}/datasets/duckdb.db"
        driver: "org.duckdb.DuckDBDriver"

    duckdb-s3:
      type: jdbc
      options:
        url: "jdbc:duckdb:{{sl_root_local}}/datasets/duckdb.db"
        driver: "org.duckdb.DuckDBDriver"
        # DuckDB S3 extension options
        s3_endpoint: "{{S3_ENDPOINT}}"
        s3_access_key_id: "{{S3_ACCESS_KEY}}"
        s3_secret_access_key: "{{S3_SECRET_KEY}}"
        s3_use_ssl: "false"
        s3_url_style: "path"
        s3_region: "us-east-1"

  # Data quality
  expectations:
    active: true

  # Metrics
  metrics:
    active: true
    discreteMaxCardinality: 10
    path: "{{SL_ROOT}}/metrics"

  # Spark configuration (if using Spark loader)
  spark:
    # Delta Lake
    sql:
      extensions: "io.delta.sql.DeltaSparkSessionExtension"
      catalog:
        spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    # Iceberg (additional config)
    # sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    # sql.catalog.spark_catalog: org.apache.iceberg.spark.SparkSessionCatalog
    # sql.catalog.spark_catalog.type: hadoop
    # sql.catalog.spark_catalog.warehouse: "{{SL_ROOT}}/warehouse"

    # Hadoop S3A configuration (for Spark S3 access)
    hadoop.fs.s3a.endpoint: "http://localhost:8333"
    hadoop.fs.s3a.access.key: "{{S3_ACCESS_KEY}}"
    hadoop.fs.s3a.secret.key: "{{S3_SECRET_KEY}}"
    hadoop.fs.s3a.path.style.access: "true"
    hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
```

---

## Connection Types Reference

### BigQuery

```yaml
connections:
  bigquery:
    type: "bigquery"
    sparkFormat: "bigquery" # Optional: use Spark connector
    options:
      location: "europe-west1" # or "us-central1"
      authType: "APPLICATION_DEFAULT" # Most common
      # authType: "SERVICE_ACCOUNT_JSON_KEYFILE"  # For service accounts
      # jsonKeyfile: "/path/to/key.json"
      # authType: "ACCESS_TOKEN"  # For short-lived tokens
      # gcpAccessToken: "TOKEN"
      authScopes: "https://www.googleapis.com/auth/cloud-platform"
      writeMethod: "direct" # or "indirect" (required with sparkFormat)
      temporaryGcsBucket: "bucket_name" # Without gs:// prefix
```

### Snowflake

```yaml
connections:
  snowflake:
    type: jdbc
    sparkFormat: snowflake # Optional: for Spark operations
    options:
      url: "jdbc:snowflake://{{SNOWFLAKE_ACCOUNT}}.snowflakecomputing.com"
      driver: "net.snowflake.client.jdbc.SnowflakeDriver"
      user: "{{SNOWFLAKE_USER}}"
      password: "{{SNOWFLAKE_PASSWORD}}"
      warehouse: "{{SNOWFLAKE_WAREHOUSE}}"
      db: "{{SNOWFLAKE_DB}}"
      keep_column_case: "off"
      preActions: "ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_LTZ'; ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = true"

      # With sparkFormat, use sf-prefixed keys:
      # sfUrl: "{{SNOWFLAKE_ACCOUNT}}.snowflakecomputing.com"
      # sfUser: "{{SNOWFLAKE_USER}}"
      # sfPassword: "{{SNOWFLAKE_PASSWORD}}"
      # sfWarehouse: "{{SNOWFLAKE_WAREHOUSE}}"
      # sfDatabase: "{{SNOWFLAKE_DB}}"
```

### Amazon Redshift

```yaml
connections:
  redshift:
    type: jdbc
    sparkFormat: "io.github.spark_redshift_community.spark.redshift"
    # On Databricks: sparkFormat: "redshift"
    options:
      url: "jdbc:redshift://account.region.redshift.amazonaws.com:5439/database"
      driver: com.amazon.redshift.Driver
      user: "{{REDSHIFT_USER}}"
      password: "{{REDSHIFT_PASSWORD}}"
      tempdir: "s3a://bucketName/data"
      tempdir_region: "eu-central-1" # Required outside AWS
      aws_iam_role: "arn:aws:iam::aws_count_id:role/role_name"
```

### PostgreSQL

```yaml
connections:
  postgresql:
    type: jdbc
    sparkFormat: jdbc # Optional: for Spark operations
    options:
      url: "jdbc:postgresql://{{POSTGRES_HOST}}:{{POSTGRES_PORT}}/{{POSTGRES_DATABASE}}"
      driver: "org.postgresql.Driver"
      user: "{{DATABASE_USER}}"
      password: "{{DATABASE_PASSWORD}}"
      quoteIdentifiers: false
```

### DuckDB

```yaml
connections:
  duckdb:
    type: jdbc
    options:
      url: "jdbc:duckdb:{{DUCKDB_PATH}}"
      driver: "org.duckdb.DuckDBDriver"
      user: "{{DATABASE_USER}}"
      password: "{{DATABASE_PASSWORD}}"
      # DuckDB S3 extension
      s3_endpoint: "{{S3_ENDPOINT}}"
      s3_access_key_id: "{{S3_ACCESS_KEY}}"
      s3_secret_access_key: "{{S3_SECRET_KEY}}"
      s3_use_ssl: "false"
      s3_url_style: "path"
      s3_region: "us-east-1"
      # DuckDB custom home directory
      # SL_DUCKDB_HOME: "{{SL_ROOT}}/.duckdb"

      # DuckDB SECRET custom home directory
      # SL_DUCKDB_SECRET_HOME: "{{SL_ROOT}}/.duckdb/stored_secrets"


```
### DuckLake

```yaml
connections:
  duckdb:
    type: jdbc
    options:
      url: "jdbc:duckdb:{{DUCKDB_PATH}}"
      driver: "org.duckdb.DuckDBDriver"
      user: "{{DATABASE_USER}}"
      password: "{{DATABASE_PASSWORD}}"

      # DuckLake (DuckDB metadata store on PostgreSQL)
      # The user should have first the postgres database
      # The user should have also created the secrets with the following SQL commands in DuckDB:
      # CREATE OR REPLACE PERSISTENT SECRET pg_{{SL_DB_ID}}
      #     (TYPE postgres, HOST '{{PG_HOST}}',PORT {{PG_PORT}}, DATABASE {{SL_DB_ID}}, USER '{{PG_USERNAME}}',PASSWORD '{{PG_PASSWORD}}')
      # CREATE OR REPLACE PERSISTENT SECRET {{SL_DB_ID}}
      #     (TYPE ducklake, METADATA_PATH '',DATA_PATH '{{SL_DATA_PATH}}', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_{{SL_DB_ID}}'});
      
      preActions: "ATTACH IF NOT EXISTS 'ducklake:{{SL_DB_ID}}' AS {{SL_DB_ID}}; USE {{SL_DB_ID}};"

      # DuckDB S3 extension if SL_DATA_PATH is on S3
      s3_endpoint: "{{S3_ENDPOINT}}"
      s3_access_key_id: "{{S3_ACCESS_KEY}}"
      s3_secret_access_key: "{{S3_SECRET_KEY}}"
      s3_use_ssl: "false"
      s3_url_style: "path"
      s3_region: "us-east-1"

      SL_DATA_PATH: "{{SL_ROOT}}/ducklake_data/{{SL_DB_ID}}" # DuckLake data path (can be on S3 or local)

      # DuckDB custom home directory
      # SL_DUCKDB_HOME: "{{SL_ROOT}}/.duckdb"
      
      # DuckDB SECRET custom home directory
      # SL_DUCKDB_SECRET_HOME: "{{SL_ROOT}}/.duckdb/stored_secrets"


```

### Apache Spark (Local/Databricks)

```yaml
connections:
  spark:
    type: "spark"
    options: {} # Any spark.* config can go in application.spark section
```

### Local Filesystem

```yaml
connections:
  local:
    type: local
```

---

## Load Configuration

### Domain Configuration (\_config.sl.yml)

```yaml
# metadata/load/sales/_config.sl.yml
load:
  name: "sales"
  directory: "{{root}}/incoming/sales" # Optional: override incoming location
  ack: "ack" # Optional: require .ack file for processing

  # Domain-level metadata (inherited by all tables)
  metadata:
    format: DSV
    separator: ","
    withHeader: true
    encoding: "UTF-8"

    writeStrategy:
      type: APPEND

    dagRef: "sales_load_dag" # Optional: custom orchestration DAG
```

### Table Configuration (table.sl.yml)

```yaml
# metadata/load/sales/orders.sl.yml
table:
  pattern: "orders_(?<mode>FULL|INCR)_.*\\.csv" # Named groups for adaptative strategy
  comment: "Sales orders table"
  primaryKey: ["order_id"]

  metadata:
    format: "DSV"
    encoding: "UTF-8"
    withHeader: true
    separator: ";"
    quote: '"'
    escape: "\\"
    filter: "^[^#].*" # Optional: filter lines (skip comments)
    ack: "ack" # Optional: require acknowledgment file
    emptyIsNull: false
    fillWithDefaultValue: false
    loader: "native" # Optional: use native database loader

    # Write strategy (see Write Strategies section)
    writeStrategy:
      type: "UPSERT_BY_KEY"
      key: ["order_id"]
      timestamp: "updated_at"
      on: TARGET

    # Sink configuration
    sink:
      connectionRef: "bigquery" # Optional: override connection

      # BigQuery: single field, Spark: list of fields
      partition:
        field: "order_date" # BigQuery
        # - "order_date"     # Spark

      clustering:
        - "customer_id"
        - "status"

      requirePartitionFilter: false # BigQuery only
      days: 90 # BigQuery: partition expiration days

      materializedView: false # BigQuery
      enableRefresh: false # BigQuery
      refreshIntervalMs: 3600000 # BigQuery: 1 hour

      format: "parquet" # Spark: parquet, delta, iceberg
      coalesce: true # Spark: reduce partitions before write

      options:
        compression: "snappy" # Spark options

    # DAG reference (overrides domain-level)
    dagRef: "orders_load_dag"

    # Spark CSV options (see Spark docs)
    options:
      dateFormat: "yyyy-MM-dd"
      timestampFormat: "yyyy-MM-dd HH:mm:ss"

  # Attributes (columns) - see Attributes section
  attributes:
    - name: "order_id"
      type: "long"
      required: true
      comment: "Unique order identifier"

    - name: "customer_id"
      type: "long"
      required: true
      foreignKey: "customers.customer_id"

    - name: "order_date"
      type: "date"
      required: true

    - name: "total_amount"
      type: "decimal"
      metric: "continuous" # or "discrete"

    - name: "email"
      type: "string"
      privacy: "SHA256" # Hash for privacy

    - name: "credit_card"
      type: "string"
      privacy: "HIDE" # Completely hide

    # Nested JSON/XML structure
    - name: "address"
      type: "struct"
      array: false
      attributes:
        - name: "street"
          type: "string"
        - name: "city"
          type: "string"
        - name: "zipcode"
          type: "string"

  # Expectations (data quality checks)
  expectations:
    - expect: "is_col_value_not_unique('order_id') => result(0) == 1"
      failOnError: true
    - expect: "is_row_count_to_be_between(1, 1000000) => result(0) == 1"
      failOnError: false

  # Access Control (Table Level)
  acl:
    - role: "roles/bigquery.dataViewer" # BigQuery
      grants:
        - "user:user@domain.com"
        - "group:group@domain.com"
        - "serviceAccount:sa@project.iam.gserviceaccount.com"
    # Spark example:
    # - role: SELECT
    #   grants:
    #     - "user@domain.com"

  # Row Level Security
  rls:
    - name: "USA only"
      predicate: "country = 'USA'"
      grants:
        - "group:usa_team"

    - name: "Recent data"
      predicate: "order_date > CURRENT_DATE - INTERVAL 90 DAY"
      grants:
        - "user:analyst@domain.com"
```

### File Format Options

| Format         | Description                       | Configuration Key    |
| -------------- | --------------------------------- | -------------------- |
| **DSV**        | Delimited values (CSV, TSV, etc.) | `format: DSV`        |
| **JSON**       | One JSON object per line          | `format: JSON`       |
| **JSON_FLAT**  | Flat JSON (no nested/repeated)    | `format: JSON_FLAT`  |
| **JSON_ARRAY** | Single array of objects           | `format: JSON_ARRAY` |
| **XML**        | XML files                         | `format: XML`        |
| **POSITION**   | Fixed-width positional            | `format: POSITION`   |

### Attribute Configuration

```yaml
attributes:
  - name: "product_id"
    type: "integer"
    required: true
    comment: "Product primary key"

    # Optional transformations
    rename: "id" # Original column name (rename in database)
    default: "0" # Default value if null
    trim: "Both" # None, Left, Right, Both
    ignore: false # Skip loading this column
    script: "UPPER(product_id)" # Transform expression

    # Relationships
    foreignKey: "products.product_id"

    # Metrics (for automatic metric computation)
    metric: "continuous" # or "discrete"

    # Privacy transformations
    privacy: "SHA256" # HIDE, MD5, SHA1, SHA256, SHA512, AES

    # BigQuery Column-Level Security
    accessPolicy: "PII" # References BigQuery policy tag

    # Fixed-width format (POSITION)
    position:
      first: 0
      last: 10
```

---

## Attribute Types Catalog

### Built-in Primitive Types

| Type             | Primitive | Pattern                                        | Example            | Use Case         |
| ---------------- | --------- | ---------------------------------------------- | ------------------ | ---------------- |
| `string`         | string    | `.+`                                           | "Hello World"      | Text fields      |
| `int`, `integer` | long      | `[-\|\\+\|0-9][0-9]*`                          | `1234`             | IDs, counts      |
| `long`           | long      | `[-\|\\+\|0-9][0-9]*`                          | `-64564`           | Large numbers    |
| `short`          | short     | `-?\\d+`                                       | `564`              | Small integers   |
| `byte`           | byte      | `.`                                            | `x`                | Single byte      |
| `double`         | double    | `[-+]?\\d*\\.?\\d+[Ee]?[-+]?\\d*`              | `-45.78`           | Floating point   |
| `decimal`        | decimal   | `-?\\d*\\.{0,1}\\d+`                           | `-45.787686786876` | Precise decimals |
| `boolean`        | boolean   | `(?i)true\|yes\|[y1]<-TF->(?i)false\|no\|[n0]` | `TruE`             | Boolean flags    |

### Date/Time Types

| Type                   | Pattern/Format           | Example                             | Use Case             |
| ---------------------- | ------------------------ | ----------------------------------- | -------------------- |
| `date`                 | yyyy-MM-dd               | `2018-07-21`                        | Standard dates       |
| `timestamp`            | ISO_DATE_TIME            | `2019-12-31 23:59:02`               | Timestamps           |
| `basic_iso_date`       | yyyyMMdd                 | `20180721`                          | Compact dates        |
| `iso_local_date`       | yyyy-MM-dd               | `2018-07-21`                        | Local dates          |
| `iso_offset_date`      | yyyy-MM-ddXXX            | `2018-07-21+02:00`                  | Dates with offset    |
| `iso_date`             | yyyy-MM-ddXXX            | `2018-07-21+02:00`                  | ISO dates            |
| `iso_local_date_time`  | yyyy-MM-ddTHH:mm:ss      | `2018-07-21T14:30:00`               | Local datetime       |
| `iso_offset_date_time` | yyyy-MM-ddTHH:mm:ssXXX   | `2018-07-21T14:30:00+02:00`         | Datetime with offset |
| `iso_zoned_date_time`  | yyyy-MM-ddTHH:mm:ss[VV]  | `2018-07-21T14:30:00[Europe/Paris]` | Datetime with zone   |
| `iso_date_time`        | ISO_DATE_TIME            | `2018-07-21T14:30:00+02:00`         | Full ISO datetime    |
| `iso_ordinal_date`     | yyyy-DDD                 | `2018-202`                          | Ordinal dates        |
| `iso_week_date`        | YYYY-Www-D               | `2018-W29-6`                        | Week dates           |
| `iso_instant`          | yyyy-MM-ddTHH:mm:ss.SSSZ | `2018-07-21T14:30:00.000Z`          | UTC instants         |
| `rfc_1123_date_time`   | RFC 1123                 | `Sat, 21 Jul 2018 14:30:00 GMT`     | HTTP dates           |

### Custom Types

```yaml
# metadata/types/custom.sl.yml
types:
  - name: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    primitiveType: "string"
    sample: "user@example.com"
    comment: "Email address format"

  - name: "phone_fr"
    pattern: "^(\\+33|0)[1-9](\\d{2}){4}$"
    primitiveType: "string"
    sample: "+33612345678"
    comment: "French phone number"

  - name: "iban"
    pattern: "^[A-Z]{2}\\d{2}[A-Z0-9]{1,30}$"
    primitiveType: "string"
    sample: "FR7630006000011234567890189"
    comment: "IBAN format"
```

---

## Write Strategies

### Strategy Comparison

| Strategy                        | Description                       | Required Options                                   | Use Case                |
| ------------------------------- | --------------------------------- | -------------------------------------------------- | ----------------------- |
| **APPEND**                      | Insert all rows                   | None                                               | Event logs, fact tables |
| **OVERWRITE**                   | Replace entire table              | None                                               | Staging, full refresh   |
| **UPSERT_BY_KEY**               | Update existing, insert new       | `key`, `on: TARGET`                                | Dimension tables        |
| **UPSERT_BY_KEY_AND_TIMESTAMP** | Upsert with timestamp check       | `key`, `timestamp`, `on: TARGET`                   | CDC, incremental        |
| **OVERWRITE_BY_PARTITION**      | Replace specific partitions       | Requires `sink.partition`                          | Partitioned fact tables |
| **DELETE_THEN_INSERT**          | Delete matching keys, then insert | `key`                                              | Transactional updates   |
| **SCD2**                        | Slowly Changing Dimension Type 2  | `key`, `timestamp`, `startTs`, `endTs`, `on: BOTH` | Historical tracking     |
| **ADAPTATIVE**                  | Runtime strategy selection        | `types: { strategy: 'condition' }`                 | Dynamic routing         |

### APPEND Strategy

```yaml
writeStrategy:
  type: "APPEND"
```

Use for: Event logs, transaction logs, append-only fact tables.

### OVERWRITE Strategy

```yaml
writeStrategy:
  type: "OVERWRITE"
```

Use for: Staging tables, full refresh scenarios, temporary tables.

### UPSERT_BY_KEY Strategy

```yaml
writeStrategy:
  type: "UPSERT_BY_KEY"
  key: ["customer_id"]
  on: TARGET # Check key in target table
```

Use for: Dimension tables, master data, configuration tables.

### UPSERT_BY_KEY_AND_TIMESTAMP Strategy

```yaml
writeStrategy:
  type: "UPSERT_BY_KEY_AND_TIMESTAMP"
  key: ["order_id"]
  timestamp: "updated_at"
  on: TARGET
```

Use for: Change Data Capture (CDC), incremental updates with timestamps.

### OVERWRITE_BY_PARTITION Strategy

```yaml
writeStrategy:
  type: "OVERWRITE_BY_PARTITION"
  on: TARGET

# Requires sink.partition configuration
sink:
  partition:
    - "year"
    - "month"
    - "day"
```

Use for: Daily/monthly partitioned fact tables.

### DELETE_THEN_INSERT Strategy

```yaml
writeStrategy:
  type: "DELETE_THEN_INSERT"
  key: ["product_id", "store_id"]
```

Use for: Transactional updates, clearing specific records before insert.

### SCD2 Strategy (Slowly Changing Dimension Type 2)

```yaml
writeStrategy:
  type: "SCD2"
  key: ["customer_id"]
  timestamp: "effective_date"
  startTs: "valid_from" # Optional, defaults to application.scd2StartTimestamp
  endTs: "valid_to" # Optional, defaults to application.scd2EndTimestamp
  on: BOTH
```

Use for: Historical tracking of dimension changes, audit trails.

**SCD2 Behavior:**

- Inserts new version with `valid_from = timestamp`, `valid_to = NULL`
- Updates previous version: `valid_to = timestamp`
- Maintains complete history of changes

### ADAPTATIVE Strategy (Dynamic Selection)

```yaml
# By day of week (full refresh on Sunday)
writeStrategy:
  types:
    APPEND: 'dayOfWeek != 7'
    OVERWRITE: 'dayOfWeek == 7'

# By filename pattern (named group)
table:
  pattern: "orders_(?<mode>FULL|INCR)_.*\\.csv"
  writeStrategy:
    types:
      OVERWRITE: 'group("mode") == "FULL"'
      APPEND: 'group("mode") == "INCR"'

# By file size (full load if large)
writeStrategy:
  types:
    OVERWRITE: 'fileSizeMo > 100'
    APPEND: 'fileSizeMo <= 100'

# By date (first day of month)
writeStrategy:
  types:
    OVERWRITE: 'isFirstDayOfMonth'
    APPEND: '!isFirstDayOfMonth'
```

**Adaptative Criteria:**

| Criteria                | Description                | Example                                      |
| ----------------------- | -------------------------- | -------------------------------------------- |
| `group(index/name)`     | Capture group from pattern | `group(1) == "F"`, `group("mode") == "FULL"` |
| `fileSize`, `fileSizeB` | File size in bytes         | `fileSize > 1000`                            |
| `fileSizeKo/Mo/Go/To`   | File size in units         | `fileSizeMo > 100`                           |
| `isFirstDayOfMonth`     | Current date check         | `isFirstDayOfMonth`                          |
| `isLastDayOfMonth`      | Current date check         | `isLastDayOfMonth`                           |
| `dayOfWeek`             | 1-7 (Mon-Sun)              | `dayOfWeek == 7`                             |
| `isFileFirstDayOfMonth` | File mod date check        | `isFileFirstDayOfMonth`                      |
| `isFileLastDayOfMonth`  | File mod date check        | `isFileLastDayOfMonth`                       |
| `fileDayOfWeek`         | File mod day 1-7           | `fileDayOfWeek == 1`                         |

---

## Transform Configuration

### Task Configuration (task.sl.yml)

```yaml
# metadata/transform/analytics/daily_sales.sl.yml
task:
  name: "daily_sales" # Optional: override filename
  domain: "analytics" # Optional: override folder name
  table: "daily_sales" # Optional: override filename

  # Write strategy (same options as load)
  writeStrategy:
    type: "OVERWRITE_BY_PARTITION"

  # Sink configuration
  sink:
    connectionRef: "bigquery" # Optional: write to different DB
    partition:
      - "report_date"
    clustering:
      - "region"
    format: "delta"
    options:
      compression: "snappy"

  # Connection for reading source data (separate from sink)
  connectionRef: "source_connection"

  # SQL parsing
  parseSQL: true # Set to false for custom INSERT/UPDATE/MERGE statements

  # Expectations (data quality)
  expectations:
    - expect: "is_row_count_to_be_between(1, 1000000) => result(0) == 1"
      failOnError: true

  # Access Control (same as load)
  acl:
    - role: SELECT
      grants:
        - "group:analytics_team"

  rls:
    - name: "Recent data"
      predicate: "report_date > CURRENT_DATE - INTERVAL 90 DAY"
      grants:
        - "user:analyst@domain.com"

  # Column descriptions (for calculated columns)
  attributesDesc:
    - name: "total_revenue"
      comment: "Sum of all sales amounts"
      accessPolicy: "SENSITIVE" # BigQuery CLS

  # DAG reference
  dagRef: "daily_analytics_dag"
```

### SQL Transform (task.sql)

```sql
-- metadata/transform/analytics/daily_sales.sql
SELECT
  DATE(order_date) as report_date,
  region,
  COUNT(*) as order_count,
  SUM(total_amount) as total_revenue,
  AVG(total_amount) as avg_order_value
FROM {{sales}}.orders
WHERE order_date BETWEEN '{{sl_start_date}}' AND '{{sl_end_date}}'
GROUP BY DATE(order_date), region
ORDER BY report_date DESC, region
```

**Template Variables:**

- `{{domain}}` - Reference other domains
- `{{sl_start_date}}`, `{{sl_end_date}}` - Incremental processing windows
- `{{table}}` - Current table name (in expectations)

### Python Transform (task.py)

```python
# metadata/transform/analytics/complex_transform.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    # Read source data
    df = spark.sql("SELECT * FROM sales.orders WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAYS")

    # Transform logic
    result_df = df.groupBy("customer_id").agg(
        count("*").alias("order_count"),
        sum("total_amount").alias("total_spent")
    ).filter(col("order_count") > 5)

    # REQUIRED: Create temporary view named SL_THIS
    result_df.createOrReplaceTempView("SL_THIS")
```

**Important:** Python transforms MUST create a temporary view named `SL_THIS`.

**Command-line options:** Passed via `--options key1=value1,key2=value2` become `--key1 value1 --key2 value2`.

---

## Expectations (Data Quality)

### Expectation Syntax

```yaml
expectations:
  - expect: "<query_name>(<params>) => <condition>"
    failOnError: true # or false to continue with warnings
```

### Built-in Expectation Macros

```yaml
# metadata/expectations/default.j2 (Jinja macros)

# Uniqueness check
{% macro is_col_value_not_unique(col, table='SL_THIS') %}
    SELECT max(cnt)
    FROM (SELECT {{ col }}, count(*) as cnt FROM {{ table }}
    GROUP BY {{ col }}
    HAVING cnt > 1)
{% endmacro %}

# Row count range
{% macro is_row_count_to_be_between(min_value, max_value, table_name = 'SL_THIS') -%}
    SELECT
        CASE
            WHEN count(*) BETWEEN {{min_value}} AND {{max_value}} THEN 1
        ELSE
            0
        END
    FROM {{table_name}}
{%- endmacro %}

# Value count
{% macro count_by_value(col, value, table='SL_THIS') %}
    SELECT count(*)
    FROM {{ table }}
    WHERE {{ col }} LIKE '{{ value }}'
{% endmacro %}
```

### Expectation Examples

```yaml
expectations:
  # Uniqueness: order_id must be unique
  - expect: "is_col_value_not_unique('order_id') => result(0) == 1"
    failOnError: true

  # Row count: between 100 and 1 million rows
  - expect: "is_row_count_to_be_between(100, 1000000) => result(0) == 1"
    failOnError: false

  # Value count: at least 10 USA records
  - expect: "count_by_value('country', 'USA') => result(0) >= 10"
    failOnError: false

  # Custom SQL: no negative amounts
  - expect: "SELECT COUNT(*) FROM SL_THIS WHERE amount < 0 => count == 0"
    failOnError: true

  # Null check: email not null
  - expect: "SELECT COUNT(*) FROM SL_THIS WHERE email IS NULL => count == 0"
    failOnError: true
```

### Available Variables in Conditions

| Variable  | Type          | Description                      |
| --------- | ------------- | -------------------------------- |
| `count`   | Long          | Number of rows in query result   |
| `result`  | Seq[Any]      | First row values (0-indexed)     |
| `results` | Seq[Seq[Any]] | All rows (for multi-row results) |

---

## Extract Configuration

### JDBC Extract

```yaml
# metadata/extract/source_db.sl.yml
version: 1
extract:
  connectionRef: "source_postgres"

  jdbcSchemas:
    - schema: "sales"

      # Custom remarks queries (for databases like DB2)
      columnRemarks: "SELECT COLUMN_NAME, COLUMN_TEXT FROM SYSCAT.COLUMNS WHERE ..."
      tableRemarks: "SELECT TABLE_TEXT FROM SYSCAT.TABLES WHERE ..."

      # Table types to extract
      tableTypes:
        - "TABLE"
        - "VIEW"
        # - "SYSTEM TABLE"
        # - "GLOBAL TEMPORARY"

      # Tables to extract
      tables:
        - name: "*" # Or specific table name or pattern

          fullExport: true # false for incremental

          # Incremental configuration
          partitionColumn: "id" # For parallel extraction
          numPartitions: 4 # Parallelism level
          timestamp: "updated_at" # For incremental tracking

          # JDBC tuning
          fetchSize: 1000 # JDBC fetch size

          # Custom query (overrides table name)
          sql: "SELECT * FROM orders WHERE region = 'EMEA'"

          # Column selection (optional)
          columns:
            - "order_id"
            - "customer_id"
            - "order_date"
            - "total_amount"
```

**Extract Command:**

```bash
# Full extraction
starlake extract --config metadata/extract/source_db.sl.yml

# Incremental (uses timestamp tracking)
starlake extract --config metadata/extract/source_db.sl.yml --incremental
```

### OpenAPI Extract

```yaml
# metadata/extract/api.sl.yml
version: 1
extract:
  openAPI:
    basePath: /api/v2

    domains:
      - name: customers_api

        # Schema filtering (regex)
        schemas:
          exclude:
            - "Model\\.Common\\.Id"
            - "Internal\\..*"

        # Route selection
        routes:
          - paths:
              include:
                - "/users"
                - "/orders"
                - "/products"
```

### Freshness Monitoring

Track data freshness with timestamp columns:

```bash
# Check freshness for specific tables
starlake freshness --tables dataset1.table1,dataset2.table2 --persist true
```

Monitoring table: `SL_LAST_EXPORT` in audit schema.

---

## DAG Configuration (Orchestration)

### DAG Configuration File

```yaml
# metadata/dags/sales_load_dag.sl.yml
dag:
  name: "sales_load_dag"
  schedule: "0 2 * * *" # Cron expression (2 AM daily)
  catchup: true # Process historical runs
  default_pool: "default_pool"
  description: "Daily sales data load from SFTP"

  # Airflow-specific
  tags:
    - "production"
    - "sales"
    - "daily"

  # Environment variables for tasks
  options:
    sl_env_var: '{"SL_ROOT": "${root_path}", "SL_DATASETS": "${root_path}/datasets", "SL_TIMEZONE": "Europe/Paris"}'

  # Load strategy (how to trigger load)
  load:
    strategy: "FILE_SENSOR" # FILE_SENSOR, FILE_SENSOR_DOMAIN, ACK_FILE_SENSOR, NONE
    options:
      incoming_path: "{{SL_ROOT}}/incoming/{{domain}}"
      pending_path: "{{SL_ROOT}}/datasets/pending/{{domain}}"
      global_ack_file_path: "{{SL_ROOT}}/datasets/pending/{{domain}}/{{{{ds}}}}.ack"

  # Custom template (optional)
  template:
    file: "custom_template.py.j2" # Relative to metadata/dags/template or absolute
```

### DAG Assignment Hierarchy

**Priority (lowest to highest):**

1. **Project level**: `application.dagRef.load` / `application.dagRef.transform`
2. **Domain level**: `load.metadata.dagRef` in `_config.sl.yml`
3. **Table level**: `table.metadata.dagRef` in `table.sl.yml`
4. **Transform level**: `task.dagRef` in `task.sl.yml`

Lower levels override higher levels.

---

## Load Strategies

### Standard Strategies

| Strategy Class                               | Description                            | Ordering     |
| -------------------------------------------- | -------------------------------------- | ------------ |
| `ai.starlake.job.load.IngestionTimeStrategy` | Load by file modification time         | Oldest first |
| `ai.starlake.job.load.IngestionNameStrategy` | Load by lexicographical filename order | Alphabetical |

Configuration:

```yaml
application:
  loadStrategyClass: "ai.starlake.job.load.IngestionNameStrategy"
```

### Custom Load Strategy

Implement `ai.starlake.job.load.LoadStrategy` interface:

```scala
package com.mycompany.starlake

import ai.starlake.job.load.LoadStrategy
import ai.starlake.storage.StorageHandler
import org.apache.hadoop.fs.Path
import java.time.LocalDateTime

object CustomLoadStrategy extends LoadStrategy with StrictLogging {
  def list(
    storageHandler: StorageHandler,
    path: Path,
    extension: String = "",
    since: LocalDateTime = LocalDateTime.MIN,
    recursive: Boolean
  ): List[FileInfo] = {
    // Custom file ordering logic
    ???
  }
}
```

```yaml
application:
  loadStrategyClass: "com.mycompany.starlake.CustomLoadStrategy"
```

---

## Metrics Configuration

### Metric Types

| Metric Type    | Computed Attributes                                                                                                            |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **continuous** | min, max, sum, mean, median, variance, stddev, skewness, kurtosis, 25th percentile, 75th percentile, missing values, row count |
| **discrete**   | count distinct, category frequency, category count (top categories), row count                                                 |

### Configuration

```yaml
# Application-level
application:
  metrics:
    active: true
    discreteMaxCardinality: 10 # Max distinct values for discrete metrics
    path: "{{SL_ROOT}}/metrics"

# Attribute-level
attributes:
  - name: "revenue"
    type: "decimal"
    metric: "continuous"

  - name: "product_category"
    type: "string"
    metric: "discrete"
```

**Metrics Storage:** Stored in audit tables (`SL_METRICS`) for historical tracking and analysis.

---

## Storage Configuration Patterns

### S3/MinIO/SeaweedFS with Spark

```yaml
# Hadoop S3A configuration
spark:
  hadoop.fs.s3a.endpoint: "http://localhost:8333"
  hadoop.fs.s3a.access.key: "{{S3_ACCESS_KEY}}"
  hadoop.fs.s3a.secret.key: "{{S3_SECRET_KEY}}"
  hadoop.fs.s3a.path.style.access: "true"
  hadoop.fs.s3a.connection.ssl.enabled: "false"
  hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
```

### DuckDB S3 Extension

```yaml
connections:
  duckdb:
    type: jdbc
    options:
      url: "jdbc:duckdb:{{DUCKDB_PATH}}"
      driver: "org.duckdb.DuckDBDriver"

      # DuckDB S3 extension
      preActions: |
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='us-east-1';
        SET s3_endpoint='localhost:8333';
        SET s3_access_key_id='{{S3_ACCESS_KEY}}';
        SET s3_secret_access_key='{{S3_SECRET_KEY}}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
```

---

## Complete Examples

### Example 1: E-Commerce Order Processing

```yaml
# metadata/application.sl.yml
version: 1
application:
  name: "ecommerce-platform"
  connectionRef: "{{activeConnection}}"
  defaultWriteFormat: delta
  timezone: "UTC"

  connections:
    spark:
      type: spark

  spark:
    sql:
      extensions: "io.delta.sql.DeltaSparkSessionExtension"
      catalog:
        spark_catalog: "org.apache.spark.sql.delta.catalog.DeltaCatalog"

# metadata/load/orders/_config.sl.yml
load:
  name: "orders"
  metadata:
    format: DSV
    separator: ","
    withHeader: true

# metadata/load/orders/transactions.sl.yml
table:
  pattern: "orders_(?<type>FULL|DELTA)_.*\\.csv"
  primaryKey: ["order_id"]

  metadata:
    writeStrategy:
      types:
        OVERWRITE: 'group("type") == "FULL"'
        UPSERT_BY_KEY_AND_TIMESTAMP: 'group("type") == "DELTA"'
      key: ["order_id"]
      timestamp: "updated_at"
      on: TARGET

    sink:
      partition:
        - "order_date"
      clustering:
        - "customer_id"
        - "status"

  attributes:
    - name: "order_id"
      type: "long"
      required: true

    - name: "customer_id"
      type: "long"
      required: true
      foreignKey: "customers.customer_id"

    - name: "order_date"
      type: "date"
      required: true

    - name: "status"
      type: "string"
      required: true
      metric: "discrete"

    - name: "total_amount"
      type: "decimal"
      required: true
      metric: "continuous"

    - name: "customer_email"
      type: "string"
      privacy: "SHA256"

    - name: "updated_at"
      type: "timestamp"
      required: true

  expectations:
    - expect: "is_col_value_not_unique('order_id') => result(0) == 1"
      failOnError: true
    - expect: "SELECT COUNT(*) FROM SL_THIS WHERE total_amount <= 0 => count == 0"
      failOnError: true

# metadata/transform/analytics/daily_revenue.sl.yml
task:
  domain: "analytics"
  table: "daily_revenue"

  writeStrategy:
    type: "OVERWRITE_BY_PARTITION"

  sink:
    partition:
      - "report_date"

  attributesDesc:
    - name: "total_revenue"
      comment: "Sum of all order amounts"

# metadata/transform/analytics/daily_revenue.sql
SELECT
  DATE(order_date) as report_date,
  COUNT(*) as order_count,
  SUM(total_amount) as total_revenue,
  AVG(total_amount) as avg_order_value,
  COUNT(DISTINCT customer_id) as unique_customers
FROM {{orders}}.transactions
WHERE order_date = '{{sl_date}}'
  AND status IN ('COMPLETED', 'SHIPPED')
GROUP BY DATE(order_date)
```

### Example 2: SCD2 Customer Dimension

```yaml
# metadata/load/customers/customer_master.sl.yml
table:
  pattern: "customers_.*\\.csv"
  primaryKey: ["customer_id"]

  metadata:
    format: DSV
    separator: ","
    withHeader: true

    writeStrategy:
      type: "SCD2"
      key: ["customer_id"]
      timestamp: "effective_date"
      startTs: "valid_from"
      endTs: "valid_to"
      on: BOTH

  attributes:
    - name: "customer_id"
      type: "long"
      required: true

    - name: "customer_name"
      type: "string"
      required: true

    - name: "email"
      type: "string"
      privacy: "SHA256"

    - name: "address"
      type: "string"

    - name: "city"
      type: "string"

    - name: "country"
      type: "string"
      metric: "discrete"

    - name: "tier"
      type: "string"
      metric: "discrete"

    - name: "effective_date"
      type: "date"
      required: true

    - name: "valid_from"
      type: "timestamp"
      comment: "SCD2 start timestamp (auto-populated)"

    - name: "valid_to"
      type: "timestamp"
      comment: "SCD2 end timestamp (auto-populated)"
```

**Result:** Maintains complete history of customer changes:

- Current row: `valid_to = NULL`
- Historical rows: `valid_to` set to change timestamp

---

## Best Practices

### 1. Use Variable Substitution Everywhere

**Good:**

```yaml
url: "jdbc:postgresql://{{PG_HOST}}:{{PG_PORT}}/{{PG_DB}}"
incoming: "{{SL_ROOT}}/incoming/{{domain}}"
```

**Bad:**

```yaml
url: "jdbc:postgresql://localhost:5432/mydb"
incoming: "/projects/100/101/incoming/sales"
```

### 2. Separate Environment Configuration

```yaml
# env.sl.yml (global)
env:
  root: "/opt/starlake"
  activeConnection: "duckdb"
  PG_HOST: "localhost"

# env.PROD.sl.yml (production overrides)
env:
  root: "/data/production/starlake"
  activeConnection: "bigquery"
  PG_HOST: "${POSTGRES_HOST}"  # Injected from env var
```

### 3. Define Reusable Custom Types

```yaml
# types/custom.sl.yml
types:
  - name: "product_sku"
    pattern: "^[A-Z]{3}-\\d{6}$"
    primitiveType: string
    sample: "PRD-123456"

  - name: "iso_country_code"
    pattern: "^[A-Z]{2}$"
    primitiveType: string
    sample: "US"
```

### 4. Layer Expectations for Data Quality

```yaml
expectations:
  # Critical: fail on error
  - expect: "is_col_value_not_unique('id') => result(0) == 1"
    failOnError: true

  # Warning: log but continue
  - expect: "is_row_count_to_be_between(100, 1000000) => result(0) == 1"
    failOnError: false
```

### 5. Choose Write Strategies by Use Case

| Table Type                   | Recommended Strategy     | Reason                   |
| ---------------------------- | ------------------------ | ------------------------ |
| **Dimension (master data)**  | `UPSERT_BY_KEY`          | Keep latest version      |
| **Dimension (with history)** | `SCD2`                   | Track changes over time  |
| **Fact (partitioned)**       | `OVERWRITE_BY_PARTITION` | Replace daily/monthly    |
| **Fact (append-only)**       | `APPEND`                 | Event logs, transactions |
| **Staging**                  | `OVERWRITE`              | Temporary, full refresh  |

### 6. Apply Privacy Transformations Early

```yaml
attributes:
  - name: "ssn"
    type: "string"
    privacy: "HIDE" # Never stored

  - name: "email"
    type: "string"
    privacy: "SHA256" # One-way hash

  - name: "ip_address"
    type: "string"
    privacy: "MD5" # Anonymize
```

### 7. Partition Large Tables

```yaml
# BigQuery
sink:
  partition:
    field: "event_date"
  clustering:
    - "user_id"
    - "event_type"
  requirePartitionFilter: true  # Force partition pruning

# Spark
sink:
  partition:
    - "year"
    - "month"
    - "day"
```

### 8. Document with Comments and Tags

```yaml
table:
  name: "orders"
  comment: "E-commerce order transactions from Shopify API"
  tags: ["revenue", "critical", "daily", "pii"]

  attributes:
    - name: "order_id"
      comment: "Unique order identifier from Shopify"
```

---

## Validation

### CLI Validation

```bash
# Validate single file
starlake validate --config metadata/application.sl.yml

# Validate all metadata
starlake validate --all

# Validate specific domain
starlake validate --domain sales
```

### IDE Integration (VS Code)

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

**Benefits:**

- Real-time validation as you type
- Auto-completion for configuration keys
- Inline documentation tooltips

---

## Troubleshooting

### Common Issues

#### 1. Schema Validation Errors

**Error:** `Missing required property: version`

**Fix:** Always include `version: 1` at the top of configuration files:

```yaml
version: 1
application: ...
```

#### 2. Connection Failures

**Error:** `Connection refused to postgres:5432`

**Check:**

- Network connectivity: `telnet postgres 5432`
- Credentials in `env.sl.yml`
- JDBC driver availability in classpath

#### 3. Write Strategy Conflicts

**Error:** `Key column 'order_id' not found`

**Fix:** Ensure key columns exist in attributes:

```yaml
writeStrategy:
  type: UPSERT_BY_KEY
  key: ["order_id"] # Must be in attributes list

attributes:
  - name: "order_id" # ✓ Present
    type: "long"
    required: true
```

#### 4. S3 Access Issues

**Error:** `Status Code: 403; Error Code: AccessDenied`

**Check:**

- S3 credentials are correct
- Endpoint URL format: `http://host:port` (no trailing slash)
- Bucket permissions allow read/write
- S3 path style: `path` vs `virtual-hosted`

#### 5. Partition Column Mismatch

**Error:** `Partition column 'date' not found in schema`

**Fix:** Partition columns must exist in attributes:

```yaml
sink:
  partition:
    - "order_date"

attributes:
  - name: "order_date" # ✓ Must exist
    type: "date"
    required: true
```

---

## Resources

- **JSON Schema**: https://www.schemastore.org/starlake.json
- **Complete Reference**: [json-schema-guide.md](reference/json-schema-guide.md)
- **Starlake Documentation**: https://docs.starlake.ai
- **GitHub**: https://github.com/starlake-ai/starlake
- **Examples**: https://github.com/starlake-ai/starlake-examples
- **Starlake Airflow**: https://github.com/starlake-ai/starlake-airflow
- **Starlake Dagster**: https://github.com/starlake-ai/starlake-dagster

---

## Version History

- **1.0.0** (2026-02-06): Initial version with core patterns and JSON Schema reference
- **2.0.0** (2026-02-06): Comprehensive update with complete environment variables catalog, all connection types, adaptative write strategies, complete attribute types, expectations framework, metrics, extract configuration, DAG patterns, and production-ready examples from official documentation
