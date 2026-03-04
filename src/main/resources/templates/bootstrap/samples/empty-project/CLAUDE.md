# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**StarBake** is a Starlake starter/demo project for an e-commerce bakery. It demonstrates data ingestion and transformation pipelines using the [Starlake](https://starlake.ai) framework. The default backend is DuckDB (configured in `metadata/application.sl.yml`).

## Repository Structure

- `metadata/application.sl.yml` — Main config: connections (DuckDB by default), audit sink, DAG references
- `metadata/env.sl.yml` — Environment variables (e.g., `activeConnection`)
- `metadata/types/` — Type definitions with regex patterns and DDL mappings across dialects (BigQuery, Snowflake, Postgres, DuckDB, Synapse)
- `metadata/load/` — Domain/table load definitions (user-created YAML + schema)
- `metadata/transform/` — SQL transform definitions (user-created YAML + SQL)
- `metadata/dags/` — DAG generation templates for Airflow, Dagster, and Snowflake across execution environments (shell, Cloud Run, Fargate, SQL)
- `metadata/expectations/` — Jinja2 data quality macros (Completeness, Schema, Volume, Uniqueness, Numeric, Validity)
- `metadata/starlake.json` — JSON Schema for all `.sl.yml` files
- `datasets/incoming/` — Landing zone for raw input data

## Key Conventions

- All config files use `.sl.yml` extension and follow `version: 1` schema defined in `starlake.json`
- Jinja2 templating is used throughout (`{{domain}}`, `{{table}}`, `{{SL_ROOT}}`, `{{activeConnection}}`)
- Type definitions in `types/default.sl.yml` map primitive types to multiple SQL dialects via `ddlMapping`
- Custom types (e.g., `email`) go in `types/types.sl.yml` with regex validation patterns

## Data Model

Four source tables with different ingestion formats:
- **Customers** (CSV) — customer_id, first_name, last_name, email, join_date
- **Orders** (JSON) — order_id, customer_id, timestamp, status, products[] (nested array)
- **Products** (JSON-ND) — product_id, name, details (nested record), ingredients[] (nested array)
- **Ingredients** (TSV) — ingredient_id, name, price, quantity_in_stock

Transformation lineage: raw tables → CustomerLifetimeValue, ProductPerformance, ProductProfitability → HighValueCustomers, TopSellingProducts, MostProfitableProducts → TopSellingProfitableProducts

## DAG Configuration

DAG files in `metadata/dags/` follow the naming pattern: `{orchestrator}_{operation}_{execution}.sl.yml`. Key options include:
- `pre_load_strategy`: none | pending | ack | imported
- `template`: references Jinja2 templates for DAG code generation
- `filename`: output filename pattern with variable substitution

The default DAG references in `application.sl.yml` are `airflow_load_shell` and `airflow_transform_shell`.