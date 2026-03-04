# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Starlake is a declarative YAML-based Extract, Load, Transform (ELT) and Orchestration tool for data pipelines. It runs on Apache Spark and supports multiple data warehouses (BigQuery, Snowflake, Redshift, DuckDB, PostgreSQL).

## Build Commands

```bash
# Compile
sbt compile

# Run all tests (slow - each test forks its own Spark context)
sbt test

# Run a single test class
sbt "testOnly *IngestJobSpec"
sbt "testOnly ai.starlake.job.ingest.IngestJobSpec"

# Run tests matching a pattern
sbt "testOnly *Integration*"

# Clean and compile
sbt cc

# Build fat JAR (excludes Spark - for use with spark-submit)
sbt assembly

# Build fat JAR with Spark embedded
sbt assemblyWithSpark

# Publish locally
sbt pl

# Format code (runs automatically on compile)
sbt scalafmt
sbt scalafmtCheck
```

## Project Structure

```
src/main/scala/ai/starlake/
├── config/       # Settings, SparkEnv, configuration loading
├── extract/      # JDBC extraction, schema inference from sources
├── job/          # Main commands: ingest, transform, load, bootstrap
│   ├── ingest/   # File ingestion (CSV, JSON, XML, Parquet)
│   ├── transform/# SQL transformations, AutoTask execution
│   └── sink/     # Output sinks (BigQuery, JDBC, ES)
├── lineage/      # Data lineage and dependency tracking
├── schema/       # Domain/table schema models, YAML parsing
├── serve/        # HTTP API server (gizmo)
├── sql/          # SQL parsing, transpilation, strategies
└── utils/        # File handling, conversion, helpers
```

Entry point: `ai.starlake.job.Main`

## Key Concepts

- **Domain**: A group of related tables (maps to schema/database)
- **Table**: Schema definition with attributes, loaded from `metadata/load/`
- **Task/Transform**: SQL-based transformation, defined in `metadata/transform/`
- **WriteStrategy**: How data is written (APPEND, OVERWRITE, UPSERT_BY_KEY, MERGE)

## Configuration

Environment variables use `SL_` prefix. Key variables:
- `SL_ROOT`: Base path for datasets/metadata
- `SL_METADATA`: Path to YAML definitions
- `SL_DATASETS`: Path to data files
- `SL_CONNECTION_REF`: Default database connection

See [env-vars.md](env-vars.md) for full list.

## Architecture Notes

- **Spark**: Core engine (3.5.x), provided at runtime
- **Tests**: Each test forks a separate JVM with its own Spark context (`Test / fork := true`)
- **Scala**: 2.13 with `-Xsource:3` flag for forward compatibility
- **YAML**: Schema definitions in `metadata/` folders
- **Connections**: Defined in `metadata/connections.sl.yml`

## Code Style

- Scalafmt 3.5.8 with `Scala213Source3` dialect
- Max line length: 100 characters
- Auto-format on compile enabled
- Use `spray` style for operator indentation

## Testing Patterns

```scala
// Tests extend TestHelper or specific base classes
class MySpec extends TestHelper {
  "feature" should "behave correctly" in {
    // Spark session available as `sparkSession`
  }
}

// Integration tests use TestContainers for DBs
class MyIntegrationSpec extends ... {
  // Uses docker containers for Postgres, etc.
}
```

Test files location: `src/test/scala/ai/starlake/`