---
sidebar_position: 140
title: kafkaload
---


## Synopsis

**comet kafkaload [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--topic:`<value>`|*Required*|Topic Name declared in reference.conf file
--format:`<value>`|*Optional*|Read/Write format eq : parquet, json, csv ... Default to parquet.
--path:`<value>`|*Required*|Source file for load and target file for store
--mode:`<value>`|*Required*|When offload is true, describes how data should be stored on disk. Ignored if offload is false.
--write-options:`<value>`|*Optional*|Options to pass to Spark Writer
--transform:`<value>`|*Optional*|Any transformation to apply to message before loading / offloading it
--offload:`<value>`|*Optional*|If true, kafka topic is offloaded to path, else data contained in path is stored in the kafka topic
--stream:`<value>`|*Optional*|Should we use streaming mode ?
--streaming-format:`<value>`|*Required*|Streaming format eq. kafka, console ...
--streaming-output-mode:`<value>`|*Required*|Output mode : eq. append ... 
--streaming-trigger:`<value>`|*Required*|Once / Continuous / ProcessingTime
--streaming-trigger-option:`<value>`|*Required*|10 seconds for example. see https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/Trigger.html#ProcessingTime-java.lang.String-
--streaming-to-table:`<value>`|*Required*|Table name to sink to
--streaming-partition-by:`<value>`|*Required*|List of columns to use for partitioning
