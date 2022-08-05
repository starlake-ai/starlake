---
sidebar_position: 70
title: parquet2csv
---


## Synopsis

**starlake parquet2csv [options]**

## Description

Convert parquet files to CSV.
The folder hierarchy should be in the form /input_folder/domain/schema/part*.parquet
Once converted the csv files are put in the folder /output_folder/domain/schema.csv file
When the specified number of output partitions is 1 then /output_folder/domain/schema.csv is the file containing the data
otherwise, it is a folder containing the part*.csv files.
When output_folder is not specified, then the input_folder is used a the base output folder.

````shell
starlake parquet2csv
         --input_dir /tmp/datasets/accepted/
         --output_dir /tmp/datasets/csv/
         --domain sales
         --schema orders
         --option header=true
         --option separator=,
         --partitions 1
         --write_mode overwrite
````


## Parameters

Parameter|Cardinality|Description
---|---|---
--input_dir:`<value>`|*Required*|Full Path to input directory
--output_dir:`<value>`|*Optional*|Full Path to output directory, if not specified, input_dir is used as output dir
--domain:`<value>`|*Optional*|Domain name to convert. All schemas in this domain are converted. If not specified, all schemas of all domains are converted
--schema:`<value>`|*Optional*|Schema name to convert. If not specified, all schemas are converted.
--delete_source:`<value>`|*Optional*|Should we delete source parquet files after conversion ?
--write_mode:`<value>`|*Optional*|One of Set(OVERWRITE, APPEND, ERROR_IF_EXISTS, IGNORE)
--option:`spark-option=value`|*Optional, Unbounded*|Any Spark option to use (sep, delimiter, quote, quoteAll, escape, header ...)
--partitions:`<value>`|*Optional*|How many output partitions

