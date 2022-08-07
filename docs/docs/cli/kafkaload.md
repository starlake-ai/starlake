---
sidebar_position: 140
title: kafkaload
---


## Synopsis

**starlake kafkaload [options]**

## Description

Two modes are available : The batch mode and the streaming mode.

### Batch mode
In batch mode, you start the kafka (off)loader regurarly and the last consumed offset 
will be stored in the `comet_offsets` topic config 
(see [reference-kafka.conf](https://github.com/starlake-ai/starlake/blob/master/src/main/resources/reference-kafka.conf#L22) for an example).

When offloading data from kafka to a file, you may ask to coalesce the result to a specific number of files / partitions.
If you ask to coalesce to a single partition, the offloader will store the data in the exact filename you provided in the path
argument.

The figure below describes the batch offloading process
![](/img/cli/kafka-offload.png)

The figure below describes the batch offloading process with `comet-offsets-mode = "FILE"`
![](/img/cli/kafka-offload-fs.png)

### Streaming mode

In this mode, te program keep running and you the comet_offsets topic is not used. The (off)loader will use a consumer group id 
you specify in the access options of the topic configuration you are dealing with.


## Parameters

Parameter|Cardinality|Description
---|---|---
--config:`<value>`|*Required*|Topic Name declared in reference.conf file
--format:`<value>`|*Optional*|Read/Write format eq : parquet, json, csv ... Default to parquet.
--path:`<value>`|*Required*|Source file for load and target file for store
--mode:`<value>`|*Required*|When offload is true, describes how data should be stored on disk. Ignored if offload is false.
--write-options:`<value>`|*Optional*|Options to pass to Spark Writer
--options:`<value>`|*Optional*|Options to pass to Spark Reader
--coalesce:`<value>`|*Optional*|Should we coalesce the resulting dataframe
--transform:`<value>`|*Optional*|Any transformation to apply to message before loading / offloading it
--offload:`<value>`|*Optional*|If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic
--streaming-format:`<value>`|*Optional*|Streaming format eq. kafka, console ...
--streaming-output-mode:`<value>`|*Optional*|Output mode : eq. append ... 
--streaming-trigger:`<value>`|*Optional*|Once / Continuous / ProcessingTime
--streaming-trigger-option:`<value>`|*Optional*|10 seconds for example. see https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/Trigger.html#ProcessingTime-java.lang.String-
--streaming-to-table:`<value>`|*Optional*|Table name to sink to
--streaming-partition-by:`<value>`|*Optional*|List of columns to use for partitioning
--stream:`<value>`|*Optional*|Should we use streaming mode ?
## Samples

### Batch offload topic from kafka to a file


Assume we want to periodically offload an avro topic to the disk and create a filename with the date time, the batch was started.
We need to provide the following input parameters to the starlake batch:

- offload: We set it to true since we are consuming data from kafka
- mode: Overwrite since we are creating a unique file for each starlake batch
- path: the file path where the consumed data will be stored. We can use here any standard starlake variable, for example `/tmp/file-{{comet_datetime}}.txt`
- format: We may save it in any spark supported format (parquet, text, json ...)
- coalesce: Write all consumed messages into a single file if set to 1
- config: The config entry on the application.conf describing the topic connections options. See below.


