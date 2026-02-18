---
name: lineage
description: Generate task dependency graphs (data lineage)
---

# Lineage Skill

Generates a visual graph of task dependencies (data lineage). Shows how transform tasks depend on each other and on source tables. Outputs to DOT, SVG, PNG, or JSON formats.

## Usage

```bash
starlake lineage [options]
```

## Options

- `--output <value>`: Output file path (default: console output)
- `--task <value>`: Comma-separated list of tasks to compute dependencies for (default: all)
- `--reload`: Reload YAML files from disk before computing lineage
- `--viz`: Generate a DOT file (Graphviz format)
- `--svg`: Generate SVG image
- `--png`: Generate PNG image
- `--json`: Generate JSON output
- `--print`: Print dependencies as text to console
- `--objects <value>`: Comma-separated list of object types to display: `task`, `table`, `view`, `unknown`
- `--all`: Include all tasks in the output (not just connected ones)
- `--verbose`: Add extra table properties to the graph
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

Lineage is computed by analyzing SQL transform files in `metadata/transform/`. Dependencies are detected from table references in SQL queries:

```sql
-- metadata/transform/kpi/order_summary.sql
-- Dependencies detected: kpi.product_summary, kpi.revenue_summary
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

The lineage graph shows:
- **Source tables** (from `load/`) as inputs
- **Transform tasks** (from `transform/`) as processing steps
- **Dependencies** between tasks as directed edges

## Examples

### Generate Full Lineage as SVG

```bash
starlake lineage --output lineage.svg --svg --all
```

### Generate Lineage for Specific Tasks

```bash
starlake lineage --task kpi.order_summary --svg --output order_lineage.svg
```

### Generate JSON Lineage

```bash
starlake lineage --json --output lineage.json --all
```

### Print Lineage as Text

```bash
starlake lineage --print --all
```

### Generate DOT File for Custom Rendering

```bash
starlake lineage --viz --output lineage.dot --all --verbose
```

### Filter by Object Types

Show only tasks and tables (exclude views):

```bash
starlake lineage --objects task,table --svg --output lineage.svg --all
```

### Generate PNG Image

```bash
starlake lineage --png --output lineage.png --all
```

## Related Skills

- [col-lineage](../col-lineage/SKILL.md) - Column-level lineage for a specific task
- [table-dependencies](../table-dependencies/SKILL.md) - Table dependency graph (FK relationships)
- [acl-dependencies](../acl-dependencies/SKILL.md) - ACL dependency graph
- [transform](../transform/SKILL.md) - Run transformation tasks
- [dag-generate](../dag-generate/SKILL.md) - Generate orchestration DAGs