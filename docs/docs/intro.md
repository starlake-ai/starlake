---
sidebar_position: 1
---

# What is Starlake ?

Starlake is a configuration based extract, load and transform engine.
The workflow below is a typical use case :
* Extract your data as a set of Fixed Position, DSV (Delimiter-separated values) or JSON or XML files
* Define or infer the structure of each POSITION/DSV/JSON/XML file with a schema using YAML syntax
* Configure the loading process
* Start watching your data being available as Tables in your warehouse.
* Build aggregates using SQL, Jinja and YAML configuration files.  


## Data Extraction
Starlake provides two options to extract, in full or incrementally, tables from your database:

1. By directly connecting to the source database and copying tables to the target warehouse. 
This approach, based on a JDBC/ODBC connection, works well for small sets of data but is not suitable for exporting high volumes of data.

2. By using the much faster and reliable bulk export feature of the source database, storing into files and load those files in the target warehouse.

## Data Loading
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
* Starlake is a very, very simple piece of software


## Data Transformation

Simply write standard SQL et describe how you want the result to be stored in a YML description file.
The major benefits Starlake bring to your Data transformation jobs are:

* Write transformations in regular SQL files
* Use Jinja2 to augment your SQL scripts and make them easier to read and maintain
* Describe where and how the result is stored using YML description files
* Apply security to the target table
* Preview your data lifecycle and publish in SVG format


## How it works

Starlake Data Pipeline automates the loading and parsing of files and
their ingestion into a warehouse where datasets become
available as strongly typed records.

![Complete Starlake Data Pipeline Workflow](/img/guide/workflow.png "Complete Starlake Data Pipeline Workflow")


The figure above describes how Starlake implements the `Extract Load Transform (ELT)` Data Pipeline steps.
Starlake may be used indistinctly for all or any of these steps.

* The `extract` step allows to export selective data from an existing SQL database to a set of CSV files.
* The `load` step allows you to load text files, to ingest POSITION/CSV/JSON/XML files as strong typed records stored as parquet files or DWH tables (eq. Google BigQuery) or whatever sink you configured
* The `transform` step allows to join loaded data and save them as parquet files, DWH tables or Elasticsearch indices

The Load Transform steps support multiple configurations for inputs and outputs as illustrated in the
figure below. 

![Anywhere](/img/guide/anywhere.png "Anywhere")


Starlake Data Pipeline steps are described below:

- Landing Area : In this optional step, files with predefined pattern filenames are stored on a local filesystem in a predefined folder hierarchy
- Pending Area : Files associated with a schema are imported into here.
- Working Area : Pending files are parsed against their schema and records are rejected or accepted and made available in parquet files as Hive Tables or Big Query tables or parquet files in a cloud bucket.
- Business Area : Tables (Hive / BigQuery / Parquet files / ...) in the working area may be joined to provide a holistic view of the data through the definition of transformations.
- Data visualization : parquet files / tables may be exposed in warehouses or elasticsearch indices through an indexing definition

Input file schemas, ingestion rules, transformation and indexing definitions used in the steps above are all defined in YAML files.

### On Premise Data Pipeline

![On Premise Workflow]( /img/guide/elt-onpremise.png )

### Azure Databricks Data Pipeline

![Azure Workflow]( /img/guide/elt-azure-databricks.png )


### Data Pipeline on Google Cloud Storage

![Cloud Storage Workflow]( /img/guide/elt-gcp-gcs.png )


### Data Pipeline on BigQuery

![Bigquery Workflow]( /img/guide/elt-gcp-bq.png )







