---
sidebar_position: 3
title: Transform
---

## Parquet to Parquet

Will load the dataset `accepted/graduateProgram` under `$COMET_DATASETS` directory from the configured storage.
An absolute path may also be specified.

This example create two views : One temporary view in the `views` section, and another one in the `presql` section.
Note that the sql request in the `presql` section uses the view defined in the `views` sectioon.

The resulting file will be stored in the `$COMET_DATASETS/business/graduateProgram/output` directory.

````yaml
---
name: "graduateProgram"
views:
  graduate_View: "fs:accepted/graduateProgram"
tasks:
  - domain: "graduateProgram"
    area: "business"
    dataset: "output"
    write: "OVERWRITE"
    presql: |
      create or replace view graduate_agg_view
      select degree,
        department,
        school
      from graduate_View
      where school={{school}}

    sql: SELECT * FROM graduate_agg_view
````

## Transform Parquet to DSV

Based ont the [parquet to parquet](#parquet-to-parquet) example, we add the format property to request a csv output
and set coalesce to `true` to output everything in a single CSV file.

````yaml
---
name: "graduateProgram"
format: "csv"
coalesce: true
views:
  graduate_View: "fs:accepted/graduateProgram"
tasks:
  - domain: "graduateProgram"
    area: "business"
    dataset: "output"
    write: "OVERWRITE"
    presql: |
      create or replace view graduate_agg_view
      select degree,
        department,
        school
      from graduate_View
      where school={{school}}

    sql: SELECT * FROM graduate_agg_view
````

## Transform Parquet to BigQuery

Based ont the [parquet to parquet](#parquet-to-parquet) example, we add the sink section to force the task to store the SQL result in BigQuery

The result will store in the current project under the `business` BigQuery dataset in the `output` table.

You may also specify the target project in the `/tasks/dataset` property using the syntax `PROJECT_ID:business`



````yaml
---
name: "graduateProgram"
views:
  graduate_View: "fs:accepted/graduateProgram"
tasks:
  - domain: "graduateProgram"
    area: "business"
    dataset: "output"
    write: "OVERWRITE"
    sink:
        type: BQ
        location: EU
    presql: |
      create or replace view graduate_agg_view
      select degree,
        department,
        school
      from graduate_View
      where school={{school}}

    sql: SELECT * FROM graduate_agg_view
````

## BigQuery to BigQuery
We may use the Spark (SPARK) or BigQuery (BQ) engine. When using the BQ engine, no spark cluster is needed.

You may want to use the Spark engine if you need to run your jobs to stay agnostic to the underlying storage or
if you need your jobs to overwrite only the partitions present in the resulting SQL.


````yaml
---
name: "graduateProgram"
views:
  graduate_View: "bq:gcp_project_id:bqdataset/graduateProgram"
tasks:
  - domain: "graduateProgram"
    sink:
        type: BQ
    area: "business"
    dataset: "output"
    write: "OVERWRITE"
    sql: SELECT * FROM graduate_View
````

## BigQuery to CSV

## BigQuery to Parquet

## Parquet to Elasticsearch

## BigQuery to Elasticsearch

## BigQuery to SQL Database

## Parquet to SQL Database

## SQL Database to SQL Database
