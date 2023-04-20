---
sidebar_position: 50
title: extract-data
---


## Synopsis

**starlake extract-data [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--mapping:`<value>`|*Required*|Database tables & connection info
--limit:`<value>`|*Optional*|Limit number of records
--numPartitions:`<value>`|*Optional*|parallelism level regarding partitionned tables
--parallelism:`<value>`|*Optional*|parallelism level of the extraction process. By default equals to the available cores: 12
--separator:`<value>`|*Optional*|Column separator
--clean:`<value>`|*Optional*|Cleanup output directory first ?
--output-dir:`<value>`|*Required*|Where to output csv files
--fullExport:`<value>`|*Optional*|Force full export to all tables
--datePattern:`<value>`|*Optional*|Pattern used to format date during CSV writing
--timestampPattern:`<value>`|*Optional*|Pattern used to format timestamp during CSV writing
--ifExtractedBefore:`<value>`|*Optional*|DateTime to compare with the last beginning extraction dateTime. If it is before that date, extraction is done else skipped.

