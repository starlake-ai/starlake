---
sidebar_position: 1
---



# What is Starlake ?

Starlake is a configuration only Extract, Load, Transform and Orchestration Declarative Data Pipeline Tool.
The workflow below is a typical use case:

* Extract your data as a set of Fixed Position, DSV (Delimiter-separated values) or JSON or XML files
* Define or infer the structure of each POSITION/DSV/JSON/XML file with a schema using YAML syntax
* Configure the loading process
* Start watching your data being available as Tables in your warehouse.
* Build aggregates using SQL, Jinja and YAML configuration files.  

Starlake may be used indistinctly for all or any of these steps.

* The `extract` step allows to export selective data from an existing SQL database to a set of CSV files.
* The `load` step allows you to load text files, to ingest FIXED-WIDTH/CSV/JSON/XML files as strong typed records stored as parquet files or DWH tables (eq. Google BigQuery) or whatever sink you configured
* The `transform` step allows to join loaded data and save them as parquet files, DWH tables or Elasticsearch indices



The figure below illustrates the typical data lifecycle in Starlake.
![](/img/workflow.png)

Input file schemas, ingestion rules, transformation and indexing definitions used in the steps above are all defined in YAML files.

## Extract

Starlake provides a simple yet powerful  way to extract, in full or incrementally, tables from your database. 

Using parallel load through a JDBC connection and configuring the incremental fields in the schema, you may extract your data incrementally.
Once copied to the cloud provider of your choice, the data is available for further processing by the Load and Transform steps.

The extraction module support two modes:

* Native mode: Native database scripts are generated and must be run against your database.
* JDBC mode: In this mode, Starlake will spawn a number of threads to extract the data. We were able to extract an average of 1 million records per second using the AdventureWorks database on Postgres.


## Load

Usually, data loading is done by writing hand made custom parsers that transform input files into datasets of records.

Starlake aims at automating this parsing task by making data loading purely declarative.

The major benefits the Starlake data loader bring to your warehouse are:

* Eliminates manual coding for data loading
* Assign metadata to each dataset
* Expose data loading metrics and history
* Transform text files to strongly typed records without coding
* Support semantic types by allowing you to set type constraints on the incoming data
* Apply privacy to specific fields
* Apply security at load time
* Preview your data lifecycle and publish in SVG format
* Support multiple data sources and sinks
* Starlake is a very, very simple piece of software to administer

Starlake loads the data using an embedded Spark engine. Please note that this mode does not require setting up a Spark cluster, it run out of the box in the starlake docker image.

:::note

For some datawarehouses like BigQuery, Starlake can make use of the datawarehouse load API to load the data. 
This is the fastest way to load the data but comes at the expense of limited features.

:::

The table below list the features supported by each mode, the one that meet your requirements depend on the quality of your source and on the audit level you wish to have:

- Import from any source
  - CSV
  - CSV with multichar separator
  - JSON Lines
  - JSON Array
  - XML
  - POSITION
  - Parquet
  - Avro
  - Kafka
  - Any JDBC database
  - Any Spark Source
  - Any char encoding including chinese, arabic, celtic ...
- Transform on load
    - rename fields
    - add new field (scripted fields)
    - Apply SQL transform on any field
    - Validate type
    - Validate pattern (semantic types)
    - Ignore some fields
    - remove fields after transformation
    - parse date and time fields in any format
    - keep filename in target table for traceability
    - Partition target table
    - Append Mode
    - Overwrite Mode
    - Merge Mode (SCD2, Remove duplicates, overwrite by key / partition ...)
    
    - Run Pre or Post Load SQL scripts
    - Report Data quality using expectations
- Save to any sink
  - Parquet
  - Delta Lake
  - Databricks
  - Google BigQuery
  - Amazon Redshift
  - Snowflake
  - Apache Hive
  - Any JDBC database
  - Any Spark Sink

- Secure your tables
    - Apply ACL
    - Apply RLS
    - Apply Tags
- Audit / Error Handling Levels
  - File level
  - Column level reporting
  - Produce replay file
  - Handle unlimited number of errors



## Transform

Simply write standard SQL et describe how you want the result to be stored in a YAML description file.
The major benefits Starlake bring to your Data transformation jobs are:

* Write transformations in regular SQL as SELECT statements only, 
Starlake will convert them to INSERT INTO, MERGE INTO or UPDATE statements depending on the write strategy you choose. 


## Orchestrate

No more need to write complex ETL scripts, just write your SQL and YAML configuration files and let Starlake do the rest on your favorite scheduler.

Starlake provides a simple yet powerful way to orchestrate your data pipeline.
Dependencies between tasks are inferred from SQL statements and YAML configuration files.
Starlake will generate a Directed Acyclic Graph (DAG) of your data pipeline and execute in the right order 
the tasks that are ready to run through your own Airflow or Dagster scheduler.
