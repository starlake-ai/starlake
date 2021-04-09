.. _cli_parquet2csv:

***************************************************************************************************
parquet2csv
***************************************************************************************************


Synopsis
--------

**comet parquet2csv [options]**


Description
-----------


| Convert parquet files to CSV.
| The folder hierarchy should be in the form /input_folder/domain/schema/part*.parquet
| Once converted the csv files are put in the folder /output_folder/domain/schema.csv file
| When the specified number of output partitions is 1 then /output_folder/domain/schema.csv is the file containing the data
| otherwise, it is a folder containing the part*.csv files.
| When output_folder is not specified, then the input_folder is used a the base output folder.
| 
| 

.. code-block:: console

   comet parquet2csv
         --input_dir /tmp/datasets/accepted/
         --output_dir /tmp/datasets/csv/
         --domain sales
         --schema orders
         --option header=true
         --option separator=,
         --partitions 1
         --write_mode overwrite

Options
-------

.. option:: --input_dir: <value>

    *Required*. Full Path to input directory


.. option:: --output_dir: <value>

    *Optional*. Full Path to output directory, if not specified, input_dir is used as output dir


.. option:: --domain: <value>

    *Optional*. Domain name to convert. All schemas in this domain are converted. If not specified, all schemas of all domains are converted


.. option:: --schema: <value>

    *Optional*. Schema name to convert. If not specified, all schemas are converted.


.. option:: --delete_source: <value>

    *Optional*. Should we delete source parquet files after conversion ?


.. option:: --write_mode: <value>

    *Optional*. One of Set(OVERWRITE, APPEND, ERROR_IF_EXISTS, IGNORE)


.. option:: --option: spark-option=value

    *Optional, Unbounded*. Any Spark option to use (sep, delimiter, quote, quoteAll, escape, header ...)


.. option:: --partitions: <value>

    *Optional*. How many output partitions


