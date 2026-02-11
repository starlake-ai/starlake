---
name: starlake-skills
description: Index of all Starlake skills
---

# Starlake Skills Library

## Reference

- [config](config/SKILL.md): Comprehensive Starlake configuration reference.

This library contains a collection of skills corresponding to Starlake CLI commands.

## Ingestion & Loading

- [autoload](autoload/SKILL.md): Automatically infer schemas and load data from the incoming directory.
- [cnxload](cnxload/SKILL.md): Load Parquet/CSV/JSON files into a JDBC table.
- [esload](esload/SKILL.md) / [index](index/SKILL.md): Load data into Elasticsearch.
- [ingest](ingest/SKILL.md): Generic data ingestion command.
- [kafkaload](kafkaload/SKILL.md): Load/Offload data to/from Kafka.
- [load](load/SKILL.md): Load data from the pending area to the data warehouse.
- [preload](preload/SKILL.md): Check for files to load in the landing area.
- [stage](stage/SKILL.md): Move files from landing to pending area.

## Transformation & Jobs

- [job](job/SKILL.md): Run a job (Alias for transform).
- [transform](transform/SKILL.md): Run a transformation task.

## Extraction

- [extract](extract/SKILL.md): Run both extract-schema and extract-data.
- [extract-bq-schema](extract-bq-schema/SKILL.md): Extract schema from BigQuery.
- [extract-data](extract-data/SKILL.md): Extract data from a database to CSV/Parquet files.
- [extract-schema](extract-schema/SKILL.md): Extract schema from a database to Starlake YAML files.
- [extract-script](extract-script/SKILL.md): Generate extraction scripts from a template.

## Schema Management & Generation

- [bootstrap](bootstrap/SKILL.md): Create a new project or bootstrap from a template.
- [infer-schema](infer-schema/SKILL.md): Infer schema from a file.
- [xls2yml](xls2yml/SKILL.md): Convert Excel domain definitions to Starlake YAML.
- [xls2ymljob](xls2ymljob/SKILL.md): Convert Excel job definitions to Starlake YAML.
- [yml2ddl](yml2ddl/SKILL.md): Generate SQL DDL from Starlake YAML.
- [yml2xls](yml2xls/SKILL.md): Convert Starlake YAML to Excel.

## Lineage & Diagrams

- [acl-dependencies](acl-dependencies/SKILL.md): Generate ACL dependencies graph.
- [col-lineage](col-lineage/SKILL.md): Generate column lineage.
- [lineage](lineage/SKILL.md): Generate task dependencies graph.
- [table-dependencies](table-dependencies/SKILL.md): Generate table dependencies graph.

## Operations & Maintenance

- [console](console/SKILL.md): Start the Starlake console.
- [freshness](freshness/SKILL.md): Check for data freshness.
- [metrics](metrics/SKILL.md): Compute metrics.
- [migrate](migrate/SKILL.md): Migrate the project to the latest version.
- [serve](serve/SKILL.md): Run the Starlake HTTP server.
- [settings](settings/SKILL.md): Print settings or test a connection.
- [validate](validate/SKILL.md): Validate the project configuration and connections.

## Security

- [iam-policies](iam-policies/SKILL.md): Apply IAM policies.
- [secure](secure/SKILL.md): Apply security policies (RLS/CLS).

## Utilities

- [bq-info](bq-info/SKILL.md): Get table information from BigQuery.
- [compare](compare/SKILL.md): Compare two Starlake project versions.
- [parquet2csv](parquet2csv/SKILL.md): Convert Parquet files to CSV.
- [site](site/SKILL.md): Generate project documentation website.
- [summarize](summarize/SKILL.md): Display table summary.
- [test](test/SKILL.md): Run integration tests.

## Airflow Integration

- [dag-deploy](dag-deploy/SKILL.md): Deploy generated DAGs.
- [dag-generate](dag-generate/SKILL.md): Generate Airflow DAGs.
