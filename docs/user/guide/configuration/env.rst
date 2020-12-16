Environment variables
#####################

By default, Comet expect metadata in the /tmp/metadata folder and will store ingested datasets in the /tmp/datasets folder.
Below is how the folders look like by default for the provided quickstart sample.

.. code::

    /tmp
    |-- datasets (Root folder of ingested datasets)
    |   |-- accepted (Root folder of all valid records)
    |   |   |-- hr (domain name as specified in the name attribute of the /tmp/metadata/hr.yml)
    |   |   |   `-- sellers (Schema name as specified in the /tmp/metadata/hr.yml)
    |   |   |       |-- _SUCCESS
    |   |   |       `-- part-00000-292c081b-7291-4797-b935-17bc9409b03b.snappy.parquet
    |   |   `-- sales
    |   |       |-- customers (valid records for this schema as specified in the /tmp/metadata/sales.yml)
    |   |       |   |-- _SUCCESS
    |   |       |   `-- part-00000-562501a1-34ef-4b94-b527-8e93bcbb5f89.snappy.parquet
    |   |       `-- orders (valid records for this schema as specified in the /tmp/metadata/sales.yml)
    |   |           |-- _SUCCESS
    |   |           `-- part-00000-92544093-4ae2-4a98-8df8-a5aba19a1b27.snappy.parquet
    |   |-- archive (Source files as found in the incoming folder are saved here after processing)
    |   |   |-- hr (Domain name)
    |   |   |   `-- sellers-2018-01-01.json
    |   |   `-- sales
    |   |       |-- customers-2018-01-01.psv
    |   |       `-- orders-2018-01-01.csv
    |   |-- business
    |   |   |-- hr
    |   |   `-- sales
    |   |-- metrics
    |   |   |-- discrete
    |   |   |-- continuous
    |   |   `-- frequencies
    |   |-- ingesting (Temporary folder used during ingestion by Comet)
    |   |   |-- hr (One temporary subfolder / domain)
    |   |   `-- sales
    |   |-- pending (Source files are copied here from the incoming folder before processing)
    |   |   |-- hr (one folder / domain)
    |   |   `-- sales
    |   |-- rejected (invalid records in processed datasets are stored here)
    |   |   |-- hr (Domain name)
    |   |   |   `-- sellers (Schema name)
    |   |   |       |-- _SUCCESS
    |   |   |       `-- part-00000-aef2dde6-af24-4e20-ad88-3e5238916e57.snappy.parquet
    |   |   `-- sales
    |   |       |-- customers
    |   |       |   |-- _SUCCESS
    |   |       |   `-- part-00000-e6fa5ff9-ad29-4e5f-a5ff-549dd331fafd.snappy.parquet
    |   |       `-- orders
    |   |           |-- _SUCCESS
    |   |           `-- part-00000-6f7ba5d4-960b-4ac6-a123-87a7ab2d212f.snappy.parquet
    |   `-- unresolved (Files found in the incoming folder but do not match any schema)
    |       `-- hr
    |           `-- dummy.json
    `-- metadata (Root of metadata files)
        |-- domains (all domain definition files are located in this folder)
        |   |-- hr.yml (One definition file / domain)
        |   `-- sales.yml
        `-- assertions (All assertion definitions go here)
        |   |-- default.comet.yml (Predefined assertion definitions)
        |   `-- assertions.comet.yml (assertion definitions defined here are accessible throughout the project)
        `-- views (All views definitions go here)
        |   |-- default.comet.yml (Predefined view definitions)
        |   `-- views.comet.yml (view definitions defined here are accessible throughout the project)
        `-- types (All semantic types are defined here)
        |   |-- default.comet.yml (Default semantic types)
        |   `-- types.comet.yml (User defined semantic types, overwrite default ones)
        `-- jobs (All transform jobs go here)
            `-- sales-by-name.yml (Compute sales by )



Almost all options are customizable through environnement vairables.
The main env vars are described below, you may change default settings. The exhaustive list of predefined env vars are
presnet in the reference.conf file.

.. csv-table::
   :widths: 25 50 25

   Env. Var, Description, Default value
   COMET_TMPDIR,"When compacting data and estimating number of partitions, Comet stores intermediates files in this folder",hdfs:///tmp/comet_tmp
   COMET_DATASETS,Once imported where the datasets are stored, eq. hdfs:///tmp/datasets
   COMET_METADATA, root fodler where domains and types metadata are stored,/tmp/metadata
   COMET_ARCHIVE,Should we archive the incoming files once they are ingested,true
   COMET_LAUNCHER,Valid values are airflow or simple,simple
   COMET_HIVE,Should be create external tables for ingested files?,true
   COMET_ANALYZE,Should we computed basic statistics (required COMET_HIVE to be set to true) ?,true
   COMET_WRITE_FORMAT,How ingested files are stored (parquet / orc / json / csv / avro),parquet
   COMET_AREA_PENDING,In $COMET_DATASET folder how the pending folder should be named,pending
   COMET_AREA_UNRESOLVED,In $COMET_DATASET folder how the unresolved folder should be named,unresolved
   COMET_AREA_ARCHIVE,In $COMET_DATASET folder how the archive folder should be named,archive
   COMET_AREA_INGESTING,In $COMET_DATASET folder how the ingesting folder should be named,ingesting
   COMET_AREA_ACCEPTED,In $COMET_DATASET folder how the accepted folder should be named,accepted
   COMET_AREA_REJECTED,In $COMET_DATASET folder how the rejected folder should be named,rejected
   COMET_AREA_BUSINESS,In $COMET_DATASET folder how the business folder should be named,business
   AIRFLOW_ENDPOINT,Airflow endpoint. Used when COMET_LAUNCHER is set to airflow,http://127.0.0.1:8080/api/experimental

.. note::
  When running on Cloudera 5.X.X prefer ORC to Parquet for the COMET_WRITE_FORMAT since Cloudera comes with Hive 1.1 which does
  not support date/timestamp fields or else simply treat dates / timestamps as strings. See HIVE_6394_


.. note::
  When running Spark on YARN in cluster mode, environment variables need to be set using the spark.yarn.appMasterEnv.[EnvironmentVariableName]


.. _HIVE_6394: https://issues.apache.org/jira/browse/HIVE-6394


