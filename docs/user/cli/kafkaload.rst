.. _cli_kafkaload:

***************************************************************************************************
kafkaload
***************************************************************************************************


Synopsis
--------

**comet kafkaload [options]**


Description
-----------




Options
-------

.. option:: --topic: <value>

    *Required*. Topic Name declared in reference.conf file


.. option:: --format: <value>

    *Optional*. Read/Write format eq : parquet, json, csv ... Default to parquet.


.. option:: --path: <value>

    *Required*. Source file for load and target file for store


.. option:: --mode: <value>

    *Required*. When offload is true, describes who data should be stored on disk. Ignored if offload is false.


.. option:: --write-options: <value>

    *Optional*. Options to pass to Spark Writer


.. option:: --transform: <value>

    *Optional*. Any transformation to apply to message before loading / offloading it


.. option:: --offload: <value>

    *Optional*. If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic


.. option:: --stream: <value>

    *Optional*. If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic


.. option:: --streaming-format: <value>

    *Required*. If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic


.. option:: --streaming-output-mode: <value>

    *Required*. If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic


.. option:: --streaming-trigger: <value>

    *Required*. Once / Continuous / ProcessingTime


.. option:: --streaming-trigger-option: <value>

    *Required*. 10 seconds for example. see https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/Trigger.html#ProcessingTime-java.lang.String-


.. option:: --streaming-to-table: <value>

    *Required*. Table name to sink to


.. option:: --streaming-partition-by: <value>

    *Required*. List of columns to use for partitioning


