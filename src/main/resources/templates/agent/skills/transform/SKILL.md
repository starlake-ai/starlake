---
name: transform
description: Run SQL or Python transformation tasks
---

# Transform Skill

Runs a defined transformation task. Tasks are SQL or Python scripts that read from source tables and write results to a target table. Tasks can have dependencies, support multiple write strategies, and can be executed recursively with all their upstream dependencies.

## Usage

```bash
starlake transform [options]
```

## Options

- `--name <value>`: Task name in the form `domain.task` (required unless `--tags` is used)
- `--compile`: Return the final compiled SQL query without executing it
- `--sync-apply`: Update YAML attributes to match the SQL query columns
- `--sync-preview`: Preview YAML attribute changes that would match the SQL query
- `--query <value>`: Run this SQL query instead of the one defined in the task file
- `--dry-run`: Dry run only — compile and validate without executing (BigQuery support)
- `--tags <value>`: Run all tasks matching these tags
- `--format`: Pretty-print the final SQL query and update the `.sql` file
- `--interactive <value>`: Run query and display results without sinking. Format: `csv`, `json`, `table`, `json-array`
- `--reload`: Reload YAML files from disk before execution (used in server mode)
- `--truncate`: Force target table truncation before insert
- `--pageSize <value>`: Number of records per page (for interactive mode)
- `--pageNumber <value>`: Page number to display (for interactive mode)
- `--recursive`: Execute all upstream dependencies recursively before this task
- `--test`: Run in test mode without committing changes
- `--accessToken <value>`: Access token for authentication (e.g. GCP)
- `--options k1=v1,k2=v2`: Substitution arguments for the SQL template
- `--scheduledDate <value>`: Scheduled date for the job, format: `yyyy-MM-dd'T'HH:mm:ss.SSSZ`
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

Transform tasks are defined in the `metadata/transform/` directory.

### Transform Domain Config (`metadata/transform/{domain}/_config.sl.yml`)

Sets default properties for all tasks in the domain:

```yaml
# metadata/transform/kpi/_config.sl.yml
version: 1
transform:
  default:
    writeStrategy:
      type: OVERWRITE
```

### SQL Transform File (`metadata/transform/{domain}/{task}.sql`)

Contains the SQL query. Use `{{domain}}.table` syntax to reference source tables:

```sql
-- metadata/transform/kpi/revenue_summary.sql
SELECT
    o.order_id,
    o.timestamp AS order_date,
    SUM(ol.quantity * ol.sale_price) AS total_revenue
FROM
    starbake.orders o
    JOIN starbake.order_lines ol ON o.order_id = ol.order_id
GROUP BY
    o.order_id, o.timestamp
```

### Task with Dependencies

Tasks can reference outputs of other tasks to form a DAG:

```sql
-- metadata/transform/kpi/order_summary.sql
SELECT
    ps.order_id,
    ps.order_date,
    rs.total_revenue,
    ps.profit,
    ps.total_units_sold
FROM
    kpi.product_summary ps
    JOIN kpi.revenue_summary rs ON ps.order_id = rs.order_id
```

### Task YAML Configuration (`metadata/transform/{domain}/{task}.sl.yml`)

Optional YAML file to configure write strategy, sink, expectations, and more:

```yaml
# metadata/transform/analytics/daily_sales.sl.yml
version: 1
task:
  domain: "analytics"
  table: "daily_sales"
  writeStrategy:
    type: "OVERWRITE_BY_PARTITION"
  sink:
    partition:
      - "report_date"
    clustering:
      - "region"
  connectionRef: "bigquery"
  expectations:
    - expect: "is_row_count_to_be_between(1, 1000000) => result(0) == 1"
      failOnError: true
  dagRef: "daily_analytics_dag"
```

### Python Transform (`metadata/transform/{domain}/{task}.py`)

Python transforms must create a temporary view named `SL_THIS`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder.getOrCreate()
df = spark.sql("SELECT * FROM sales.orders")
result = df.groupBy("customer_id").agg(
    count("*").alias("order_count"),
    sum("total_amount").alias("total_spent")
)
result.createOrReplaceTempView("SL_THIS")
```

## Examples

### Run a Single Task

```bash
starlake transform --name kpi.revenue_summary
```

### Compile SQL Only (Debug)

View the final compiled SQL without executing:

```bash
starlake transform --name kpi.order_summary --compile
```

### Run with Recursive Dependencies

Execute the task and all its upstream dependencies:

```bash
starlake transform --name kpi.order_summary --recursive
```

### Interactive Query (Preview Results)

Run and display results as a table without writing to the target:

```bash
starlake transform --name kpi.revenue_summary --interactive table
```

### Interactive with Pagination

```bash
starlake transform --name kpi.revenue_summary --interactive json --pageSize 50 --pageNumber 1
```

### Dry Run (BigQuery)

```bash
starlake transform --name kpi.order_summary --dry-run
```

### Run All Tasks with a Tag

```bash
starlake transform --tags daily
```

### Run with Custom Options

Pass substitution variables to the SQL template:

```bash
starlake transform --name kpi.revenue_summary --options start_date=2024-01-01,end_date=2024-03-31
```

### Sync YAML Attributes from SQL

Automatically update the task YAML attributes to match the SQL query output columns:

```bash
starlake transform --name kpi.revenue_summary --sync-apply
```

### Pretty-Print and Format SQL

```bash
starlake transform --name kpi.revenue_summary --format
```

### Test Transform

```bash
starlake transform --name kpi.revenue_summary --test
```

## Related Skills

- [load](../load/SKILL.md) - Load raw data before transforming
- [lineage](../lineage/SKILL.md) - Visualize task dependency graphs
- [col-lineage](../col-lineage/SKILL.md) - Column-level lineage for a task
- [dag-generate](../dag-generate/SKILL.md) - Generate orchestration DAGs
- [test](../test/SKILL.md) - Run integration tests for transforms
- [config](../config/SKILL.md) - Configuration reference (write strategies, connections)