***************************************************************************************************
esload | index
***************************************************************************************************


Synopsis
--------

**comet index | esload [options]**


Description
-----------




Options
-------

.. option:: --timestamp: <value>

    *Optional*. Elasticsearch index timestamp suffix as in {@timestamp|yyyy.MM.dd}


.. option:: --id: <value>

    *Optional*. Elasticsearch Document Id


.. option:: --mapping: <value>

    *Optional*. Path to Elasticsearch Mapping File


.. option:: --domain: <value>

    *Required*. Domain Name


.. option:: --schema: <value>

    *Required*. Schema Name


.. option:: --format: <value>

    *Required*. Dataset input file : parquet, json or json-array


.. option:: --dataset: <value>

    *Optional*. Input dataset path


.. option:: --conf: es.batch.size.entries=1000,es.batch.size.bytes=1mb...

    *Optional*. eshadoop configuration options.
    See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
    


