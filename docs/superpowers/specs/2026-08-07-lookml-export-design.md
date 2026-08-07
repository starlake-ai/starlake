# LookML Export for Semantic Models

Date: 2026-08-07
Status: Approved

## Goal

Extend the existing `semantic-export` command (currently Apache Ossie only) with a
`lookml` format that converts the Snowflake-style semantic models stored in
`metadata/semantic/` into a ready-to-use Looker project (view files plus a model
file with explores).

## Non-goals

- No typed intermediate semantic model shared across formats. The LookML
  converter is a sibling of `OssieConverter`, operating directly on the parsed
  YAML `JsonNode`. A shared model may be extracted later when a third format
  (TMDL) gives it two concrete consumers.
- No translation of `verified_queries` (skipped with a log message) and no
  functional translation of semantic `filters` (emitted as comments).
- No SQL dialect rewriting beyond simple aggregate recognition.

## CLI and config

- `--format lookml` accepted; validation becomes `ossie | lookml`.
- New optional `--connection <name>` flag on `SemanticExportConfig`
  (`connection: Option[String] = None`). When absent, fall back to the
  project's active `connectionRef` from settings. This value fills the
  `connection:` line of the model file.
- Default output directory unchanged: `metadata/semantic/export/<format>/`.
  LookML writes each model into its own subfolder:
  `export/lookml/<modelName>/`.
- Command help text updated to document the new format and flag.

## Architecture

New pure converter in `ai.starlake.semantic`:

```scala
object LookMLConverter {
  def convert(modelName: String, model: JsonNode, connection: String): Seq[(String, String)]
}
```

It returns relative-path/content pairs:

```
<modelName>.model.lkml
<table1>.view.lkml
<table2>.view.lkml
...
```

`SemanticExportJob.run()` dispatches on `config.format`:

- `ossie`: current behavior, unchanged.
- `lookml`: resolve the connection name, call `LookMLConverter.convert`, write
  each returned file under `outputDir/<modelName>/`.

## View files (one per table)

- `sql_table_name:` from `base_table` as `database.schema.table`; falls back to
  the table name when `base_table` is absent.
- **Dimensions** become `dimension` blocks:
  - `sql:` from `expr`, falling back to `${TABLE}.<name>`.
  - `type:` mapped from `data_type`: NUMBER/INT/DECIMAL/FLOAT family to
    `number`, TEXT/STRING/VARCHAR/CHAR to `string`, BOOLEAN to `yesno`.
    Unknown or absent types omit the `type:` parameter (LookML defaults to
    string).
  - `description` copied. Synonyms are appended to the description as
    `Synonyms: a, b`.
  - `sample_values` become LookML `suggestions: [...]`.
- **Primary key**: the dimension matching a single-column `primary_key` gets
  `primary_key: yes`. Composite primary keys are not representable in LookML;
  emit a comment in the view and skip.
- **Time dimensions** become `dimension_group` blocks with `type: time`,
  `timeframes: [raw, date, week, month, quarter, year]`, and `datatype: date`
  when the source `data_type` is DATE.
- **Facts** become `dimension` blocks with `type: number`.
- **Table metrics** become `measure` blocks (see Measure translation).
- **Filters**: emitted as a comment block in the view (name and expr), since
  named filters have no direct LookML equivalent.
- `is_enum` and `access_modifier` have no mapping and are ignored.

## Measure translation

A small pattern recognizer maps common aggregates to native LookML measure
types:

| Semantic expr             | LookML                                  |
|---------------------------|-----------------------------------------|
| `SUM(x)`                  | `type: sum`, `sql: x`                   |
| `AVG(x)`                  | `type: average`, `sql: x`               |
| `MIN(x)` / `MAX(x)`       | `type: min` / `type: max`, `sql: x`     |
| `COUNT(*)`                | `type: count` (no sql)                  |
| `COUNT(DISTINCT x)`       | `type: count_distinct`, `sql: x`        |
| anything else             | `type: number`, raw SQL, review comment |

Matching is case-insensitive on the aggregate function name and requires the
whole expression to be a single aggregate call. When the argument is a bare
column name belonging to the same view, `sql:` uses the substitution operator
`${column}`; otherwise the argument is emitted verbatim.

**Model-level metrics** attach to the view of the table their aggregate
argument references (`COUNT(DISTINCT customers.customer_id)` lands in the
`customers` view with `sql: ${customer_id}`). When the owning table cannot be
determined, the metric goes into the first view with the `type: number`
fallback and a review comment.

## Model file

`<modelName>.model.lkml` contains, in order:

1. `connection: "<name>"`
2. `include: "*.view.lkml"`
3. Explores: one `explore` per distinct `left_table` across relationships,
   containing one `join` per relationship with that left table:
   - `type:` from `join_type`, defaulting to `left_outer`.
   - `relationship:` from `relationship_type`, defaulting to `many_to_one`.
   - `sql_on:` from `relationship_columns` as
     `${left.colL} = ${right.colR}` terms joined with `AND`.
4. A bare `explore` for every table that appears in no relationship (neither
   side).

## Identifier sanitization

LookML identifiers must match `[a-z0-9_]+`. All emitted names (views,
dimensions, measures, explores) are lowercased and invalid characters replaced
with `_`. A warning is logged whenever sanitization changes a name. Join
`sql_on` references and `${...}` substitutions use the sanitized names.

## Rendering

Plain string building (no template engine): 2-space indentation, `sql:`
parameters terminated with ` ;;`, double-quoted strings with `"` escaped.
Comments use `#`.

## Error handling

- Unknown `--format` values keep failing at CLI validation.
- A model with no tables produces a model file with no explores and no view
  files (same tolerance as the Ossie exporter).
- Missing optional attributes never fail the export; the converter degrades to
  fallbacks described above.

## Testing

New `LookMLExportSpec` extending `TestHelper`, reusing the same sample model
YAML as `SemanticExportSpec`:

- The three files are produced under
  `metadata/semantic/export/lookml/ecommerce_analytics/`.
- Model file: connection line, include line, `explore: orders` joining
  `customers` with `type: left_outer`, `relationship: many_to_one`, and
  `sql_on: ${orders.customer_id} = ${customers.customer_id}`.
- Orders view: `sql_table_name: ANALYTICS_DB.ECOMMERCE.ORDERS`,
  `primary_key: yes` on `order_id`, `dimension_group: order_date` with
  `datatype: date`, `suggestions` from `sample_values` on `order_status`,
  `measure: avg_order_value` with `type: average` and `sql: ${order_total}`.
- Customers view: `measure: customer_count` with `type: count_distinct`
  (model-level metric attached to the owning view).
- Fallback case: a metric with a non-trivial expression produces
  `type: number` with the raw SQL and a review comment.
- Sanitization case: a mixed-case table or column name is lowercased in the
  output.
- `--connection` flag: explicit value wins over the settings fallback.
