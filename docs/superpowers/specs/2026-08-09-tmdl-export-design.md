# Power BI TMDL Export for Semantic Models

Date: 2026-08-09
Status: Approved

## Goal

Extend the `semantic-export` command (formats today: ossie, lookml) with a
`tmdl` format that converts the Snowflake-style semantic models stored in
`metadata/semantic/` into a Power BI TMDL folder (Tabular Model Definition
Language) consumable by Tabular Editor, pbi-tools, and Power BI developer
workflows.

Work continues on branch `feat/semantic-export-lookml`; both formats ship
together.

## Non-goals

- No typed intermediate semantic model. TMDLConverter operates on the parsed
  YAML `JsonNode` like its siblings. Only small format-neutral helpers are
  extracted for reuse (see Shared helpers).
- No general SQL-to-DAX translation beyond the simple aggregate patterns
  already recognized by `parseAggregate`.
- No translation of `verified_queries` or semantic `filters` (logged as
  skipped; TMDL has no safe free-comment construct to carry them).
- No linguistic schema: synonyms are appended to descriptions, as in the
  LookML export.

## CLI and config

- `--format tmdl` accepted; validation becomes `ossie | lookml | tmdl`.
- The existing `--connection` flag is reused with per-format meaning:
  - lookml: Looker connection label written into the model file (unchanged).
  - tmdl: name of the **Starlake connection** (in `application.sl.yml`) whose
    type drives Power Query source derivation.
  - Default for both: the project's active `connectionRef`.
- Help text updated accordingly.
- Output directory follows the LookML convention:
  `metadata/semantic/export/tmdl/<modelName>/`.

## Shared helpers (approach C)

New `private[semantic]` object `SemanticModelOps` in `ai.starlake.semantic`
receives the format-neutral pieces currently inside `LookMLConverter`:

- `elems(node, key): List[JsonNode]` and `text(node, key): Option[String]`
- `ParsedAggregate` with its field renamed `lookmlType` to `aggregate`
  (values unchanged: sum, average, min, max, count, count_distinct) since it
  is no longer LookML-specific; LookML call sites use positional
  construction and pattern matching, so only the declaration changes
- `parseAggregate(expr): Option[ParsedAggregate]`
- `IdentifierPattern` and `QualifiedPattern` regexes
- `combinedDescription(node): Option[String]` (LookML's description+synonyms
  combiner, renamed) and `baseTableFqn(table): String` (LookML's `source`,
  renamed), both format-neutral
- `assignModelMetrics(model, tableNames: List[String], normalize: String => String): Map[String, List[(JsonNode, Boolean)]]`
  where keys are normalized table names and the Boolean is the ownership flag
  (false = owning table could not be determined; such metrics map to the first
  table).

`LookMLConverter` delegates to `SemanticModelOps` with `normalize = sanitize`.
`TMDLConverter` uses `normalize = _.toLowerCase` (original names are kept in
output; qualifier matching is case-insensitive). `OssieConverter` is
untouched. The existing `LookMLConverterSpec` keeps every assertion
unchanged; only its imports of the moved symbols (`parseAggregate`,
`ParsedAggregate`) switch to `SemanticModelOps`. All LookML and Ossie suites
passing is the refactor's regression proof.

## Output layout

```
export/tmdl/<modelName>/
├── database.tmdl        # database <modelName> + compatibilityLevel: 1600
├── model.tmdl           # model Model, culture: en-US, model description
├── relationships.tmdl   # only when the model has relationships
└── tables/<table>.tmdl  # one file per table
```

No `expressions.tmdl`: server/database values are baked into each partition
rather than shared M parameters.

## Naming and quoting

- Object names are kept verbatim (TMDL allows spaces and mixed case).
- In TMDL declarations and references, a name is single-quoted when it does
  not match `[A-Za-z0-9_]+`.
- In DAX, table references are always quoted (`'orders'[order_total]`) and
  `]` inside a column reference is escaped as `]]`.
- In M string literals and TMDL description text, `"` is escaped by doubling
  per M rules; the SQL embedded in `Value.NativeQuery` is escaped the same
  way.
- In the partition's native query, a field NAME that is not a plain
  identifier is wrapped in ANSI double quotes (both bare and as the `AS`
  alias) so the generated `SELECT` stays valid SQL; the `expr` side is left
  untouched.

## Table files

Each `tables/<table>.tmdl` contains, in order: doc comment, `table` line,
columns, generated key columns (composite relationships), measures, partition.

- **Columns** from `dimensions`, `time_dimensions`, `facts`:
  - `dataType:` mapped from `data_type`: INT/INTEGER/BIGINT/SMALLINT to
    `int64`; NUMBER/NUMERIC/DECIMAL to `decimal`; FLOAT/DOUBLE/REAL to
    `double`; TEXT/STRING/VARCHAR/CHAR to `string`; BOOLEAN/BOOL to
    `boolean`; DATE/DATETIME/TIME/TIMESTAMP/TIMESTAMP_NTZ/TIMESTAMP_LTZ/
    TIMESTAMP_TZ to `dateTime`; unknown or absent to `string`.
  - `sourceColumn: <field name>` for every column (the partition query
    aliases expressions, see Partition).
  - `summarizeBy: sum` for facts whose dataType mapped to int64, decimal, or
    double; `summarizeBy: none` otherwise (all dimensions and time
    dimensions included).
  - Descriptions become `///` doc lines above the column; synonyms are
    appended as `Synonyms: a, b` (same combination rule as LookML:
    `"<desc>. Synonyms: ..."`, or the synonyms part alone).
- **Primary key**: the column matching a single-column `primary_key` gets
  `isKey`. Composite primary keys emit nothing and log a warning.
- `sample_values`, `is_enum`, `access_modifier` are ignored. `filters` are
  logged as skipped.
- **Partition**: one import-mode M partition named after the table:

  ```
  partition <table> = m
      mode: import
      source =
          let
              <source lines from connection derivation>
              Result = Value.NativeQuery(<source ref>, "<sql>")
          in
              Result
  ```

  The SQL is `SELECT <expr> AS <name>, ... FROM <fqn>` listing every
  dimension, time dimension, and fact (`expr` falling back to the field name
  when absent), where `<fqn>` is `database.schema.table` from `base_table`,
  falling back to the table name. When the table has no fields the query is
  `SELECT * FROM <fqn>`. Computed expressions therefore live in the source
  query, and no DAX calculated columns are needed for them.

## Power Query source derivation

A private resolver inside the converter maps the resolved
`Option[ConnectionInfo]` to M source lines, keyed on the connection's engine
name (BigQuery via `isBigQuery`; otherwise the JDBC URL scheme, replicating
`getJdbcEngineName`'s derivation without constructing an `Engine`, so
unmapped names degrade to the generic fallback instead of failing):

| Engine | M source |
|---|---|
| postgresql | `PostgreSQL.Database("host:port", "db")` |
| redshift | `AmazonRedshift.Database("host:port", "db")` |
| mysql | `MySQL.Database("host:port", "db")` |
| sqlserver | `Sql.Database("host", "db")` |
| snowflake | `Snowflake.Databases("account-host", "<warehouse>")` then `DB = Source{[Name="<database>"]}[Data]` navigation; NativeQuery targets `DB` |
| bigquery | `GoogleBigQuery.Database([BillingProject="<project>"])` |
| duckdb, unknown engine, connection not found or None | `Sql.Database("SERVER_TODO", "DATABASE_TODO")` preceded by an M `// TODO Starlake: set the connector for your warehouse` comment |

- Host, port, and database are parsed from the JDBC URL in
  `options("url")` (`jdbc:<engine>://host[:port][/db][?params]`); a missing
  database component becomes `DATABASE_TODO`.
- Snowflake: account host from `sfUrl`/`url`; warehouse from options
  `warehouse` or `sfWarehouse`, else `WAREHOUSE_TODO`; the navigated
  database is the `base_table` database of the first table, else
  `DATABASE_TODO`.
- BigQuery: billing project from option `gcpProjectId`, else the
  `base_table` database component, else `PROJECT_TODO`.
- Derivation never fails the export: missing pieces degrade to `*_TODO`
  placeholders inside the M expression.

## Measures (DAX translation)

Table-level metrics render in their table's file; model-level metrics use the
shared ownership assignment (qualified aggregate argument names the table).
Native translation when `parseAggregate` succeeds, the metric is owned, and
the (qualifier-stripped) argument names a column of the table:

| Semantic expr | DAX |
|---|---|
| `SUM(x)` | `SUM('table'[x])` |
| `AVG(x)` | `AVERAGE('table'[x])` |
| `MIN(x)` / `MAX(x)` | `MIN('table'[x])` / `MAX('table'[x])` |
| `COUNT(*)` | `COUNTROWS('table')` |
| `COUNT(DISTINCT x)` | `DISTINCTCOUNT('table'[x])` |

Everything else (unparseable expression, argument not a column of the table,
or unowned model metric, which goes to the first table) falls back to:

```
/// TODO Starlake: translate original SQL to DAX: <original expr>
measure <name> = BLANK()
```

Metric descriptions and synonyms become `///` doc lines above the measure
(before the TODO line when both apply).

## Relationships

`relationships.tmdl` contains one entry per relationship:

```
relationship <name>
    fromColumn: '<left_table>'.'<left_column>'
    toColumn: '<right_table>'.'<right_column>'
```

(quoting per the naming rules; unquoted when `[A-Za-z0-9_]+`).

- `relationship_type` many_to_one or absent: no cardinality properties (the
  engine default is many-to-one). Any other value: log a warning and emit
  default properties.
- `join_type` has no TMDL equivalent and is ignored.
- **Composite relationships** (more than one column pair): both tables get a
  hidden calculated key column named `_sl_<relationship name>_key`:

  ```
  column _sl_<relname>_key = COMBINEVALUES("|", [c1], [c2], ...)
      dataType: string
      isHidden
      summarizeBy: none
  ```

  and the relationship joins the two generated columns. Column order follows
  the relationship_columns order, left columns on the left table, right
  columns on the right table.
- A relationship with empty `relationship_columns` is skipped with a warning.

## Job dispatch

`SemanticExportJob` gains `case "tmdl"`:

1. `name = config.connection.getOrElse(settings.appConfig.connectionRef)`
2. `info = settings.appConfig.connections.get(name)`; when absent, log a
   warning (export proceeds with the generic fallback source).
3. `TMDLConverter.convert(modelName, model, info): Seq[(String, String)]`
   returns relative paths (`database.tmdl`, `model.tmdl`,
   `relationships.tmdl`, `tables/<table>.tmdl`) and contents.
4. Files are written under `outputDir/<modelName>/`; the storage handler
   creates the `tables/` subdirectory on write.

Ossie and lookml paths are unchanged.

## Error handling

- Unknown `--format` values keep failing at CLI validation.
- A model with no tables produces `database.tmdl` and `model.tmdl` only.
- Missing optional attributes never fail the export; fallbacks are described
  above.

## Testing

- **`TMDLConverterSpec`** (pure, AnyFlatSpec): folder layout and file set;
  column dataType/sourceColumn/summarizeBy/isKey rendering; description and
  synonyms doc lines; native-query SQL with `expr AS name` and base_table
  fqn; per-engine source derivation (postgresql URL parsing, snowflake
  navigation and warehouse option, bigquery billing project, unknown-engine
  and no-connection fallback); DAX translations for all five patterns; BLANK
  fallback with TODO doc line (unparseable and unowned cases); composite
  relationship generating COMBINEVALUES key columns on both tables plus the
  relationship entry; single-column relationship rendering with quoting of
  non-word names; M/DAX/TMDL escaping of quotes and brackets.
- **`SemanticExportCmdSpec`**: `--format tmdl` accepted; unknown formats
  still rejected.
- **`TMDLExportSpec`** (integration, TestHelper): end-to-end export writes
  the four files under `export/tmdl/<modelName>/`; unknown connection name
  falls back with a warning; ossie regression stays green.
- **Refactor regression**: `LookMLConverterSpec`, `LookMLExportSpec`, and
  `SemanticExportSpec` pass unchanged after the `SemanticModelOps`
  extraction.
