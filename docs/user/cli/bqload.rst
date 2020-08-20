***************************************************************************************************
bqload
***************************************************************************************************


Synopsis
--------

**comet bqload [options]**


Description
-----------




Options
-------

.. option:: --source_file: <value>

    *Required*. Full Path to source file


.. option:: --output_dataset: <value>

    *Required*. BigQuery Output Dataset


.. option:: --output_table: <value>

    *Required*. BigQuery Output Table


.. option:: --output_partition: <value>

    *Optional*. BigQuery Partition Field


.. option:: --require_partition_filter: <value>

    *Optional*. Require Partition Filter


.. option:: --output_clustering: col1,col2...

    *Optional*. BigQuery Clustering Fields


.. option:: --source_format: <value>

    *Optional*. Source Format eq. parquet. This option is ignored, Only parquet source format is supported at this time


.. option:: --create_disposition: <value>

    *Optional*. Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition


.. option:: --write_disposition: <value>

    *Optional*. Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition


.. option:: --row_level_security: <value>

    *Optional*. value is in the form name,filter,sa:sa@mail.com,user:user@mail.com,group:group@mail.com 


