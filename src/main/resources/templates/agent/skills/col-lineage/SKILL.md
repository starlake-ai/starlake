---
name: col-lineage
description: Generate column-level lineage for a specific task
---

# Column Lineage Skill

Generates column-level lineage for a specific transform task, showing how each output column traces back to its source columns through SQL transformations. This provides fine-grained data provenance information.

## Usage

```bash
starlake col-lineage [options]
```

## Options

- `--task <value>`: Task name in the form `domain.task` (required)
- `--output <value>`: Output JSON file path (default: console output)
- `--accessToken <value>`: Access token for authentication (e.g. GCP)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## How It Works

Analyzes the SQL query for a task and traces each output column back to its source columns. For example, given:

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

The column lineage output would show:
- `order_id` ← `starbake.orders.order_id`
- `order_date` ← `starbake.orders.timestamp`
- `total_revenue` ← `starbake.order_lines.quantity`, `starbake.order_lines.sale_price`

## Examples

### Generate Column Lineage for a Task

```bash
starlake col-lineage --task kpi.revenue_summary
```

### Save Column Lineage to File

```bash
starlake col-lineage --task kpi.order_summary --output col_lineage.json
```

### Column Lineage with Access Token

```bash
starlake col-lineage --task kpi.revenue_summary --accessToken $GCP_TOKEN
```

## Related Skills

- [lineage](../lineage/SKILL.md) - Task-level dependency graph
- [table-dependencies](../table-dependencies/SKILL.md) - Table relationship graph
- [transform](../transform/SKILL.md) - Run transformation tasks