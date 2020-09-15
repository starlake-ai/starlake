***************************************************************************************************
sqlload
***************************************************************************************************


Synopsis
--------

**comet sqlload [options]**


Description
-----------




Options
-------

.. option:: --source_file: <value>

    *Required*. Full Path to source file


.. option:: --output_table: <value>

    *Required*. JDBC Output Table


.. option:: --driver: <value>

    *Optional*. JDBC Driver to use


.. option:: --partitions: <value>

    *Optional*. Number of Spark Partitions


.. option:: --batch_size: <value>

    *Optional*. JDBC Batch Size


.. option:: --user: <value>

    *Optional*. JDBC user


.. option:: --password: <value>

    *Optional*. JDBC password


.. option:: --url: <value>

    *Optional*. Database JDBC URL


.. option:: --create_disposition: <value>

    *Optional*. Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition


.. option:: --write_disposition: <value>

    *Optional*. Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition


