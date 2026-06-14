# Starlake vs. dbt, Fivetran/Airbyte, Matillion/Talend/Informatica

A side-by-side look at where Starlake fits in the modern data stack.
This page is maintained by hand and reviewed quarterly. If a cell is
wrong, open a PR — every claim is meant to be defensible.

## Legend

| Symbol | Meaning |
|---|---|
| ✅ | First-class support, available out of the box |
| ⚠️ | Partial support, requires an extra tool, or has a significant caveat (footnoted below) |
| ❌ | Not supported |

Numbered superscripts (¹, ², …) refer to footnotes at the bottom of the page.

## 1. ELT lifecycle coverage

| Capability | Starlake | dbt | Fivetran/Airbyte | Matillion/Talend/Informatica |
|---|---|---|---|---|
| Extract from operational DBs (JDBC) | ✅ | ❌ | ✅ | ✅ |
| Load raw files (CSV/JSON/XML/Parquet) | ✅ | ❌ | ⚠️¹ | ✅ |
| Declarative schema validation on ingest | ✅ | ❌ | ⚠️² | ⚠️³ |
| Merge strategies (UPSERT / SCD2 / APPEND) | ✅ | ⚠️⁴ | ❌ | ✅ |
| SQL transforms | ✅ | ✅ | ❌ | ✅ |
| Python transforms | ⚠️⁵ | ⚠️⁶ | ❌ | ✅ |
| Orchestration / DAG generation | ✅ | ⚠️⁷ | ❌ | ✅ |

## 2. Engines & warehouses

| Engine | Starlake | dbt | Fivetran/Airbyte | Matillion/Talend |
|---|---|---|---|---|
| BigQuery / Snowflake / Redshift / Databricks / Postgres | ✅ | ✅ | ✅ | ✅ |
| DuckDB (local dev) | ✅ | ✅ | ❌ | ❌ |
| Spark (self-managed) | ✅ | ⚠️⁸ | ❌ | ⚠️⁹ |
| Delta Lake / Iceberg | ✅ | ⚠️¹⁰ | ❌ | ⚠️ |

## 3. Quality & governance

| Capability | Starlake | dbt | Fivetran/Airbyte | Matillion |
|---|---|---|---|---|
| Declarative expectations / tests | ✅ | ✅ | ❌ | ⚠️ |
| Table-level lineage | ✅ | ✅ | ⚠️ | ⚠️ |
| Column-level lineage | ✅ | ⚠️¹¹ | ❌ | ⚠️ |
| Row-level security (RLS) | ✅ | ❌ | ❌ | ⚠️ |
| Column-level security (CLS) | ✅ | ❌ | ❌ | ⚠️ |
| IAM / policy tags | ✅ | ❌ | ❌ | ⚠️ |

## 4. Developer experience

| Trait | Starlake | dbt | Fivetran/Airbyte | Matillion |
|---|---|---|---|---|
| Open source | ✅ | ✅¹² | ✅¹³ | ❌ |
| Self-hosted option | ✅ | ✅ | ⚠️ | ⚠️ |
| Declarative YAML config | ✅ | ⚠️¹⁴ | ❌ | ❌ |
| Local dev (no warehouse needed) | ✅ | ✅¹⁵ | ❌ | ❌ |
| CI-friendly (text-based, diffable) | ✅ | ✅ | ❌ | ⚠️ |
| GUI editor | ⚠️¹⁶ | ⚠️ | ✅ | ✅ |