---
sidebar_position: 70
title: extract-data
---


## Synopsis

**starlake extract-data [options]**

## Description

Extract data from any database defined in mapping file.

Extraction is done in parallel by default and use all the available processors. It can be changed using `parallelism` CLI config.
Extraction of a table can be divided in smaller chunk and fetched in parallel by defining partitionColumn and its numPartitions.

Examples
========

Objective: Extract data and customize timestamp to have higher precision.

  starlake.sh extract-data --config my-config --output-dir $PWD/output --timestampPattern "yyyy-MM-dd HH:mm:ss.SSSSSS"

Objective: Plan to fetch all data but with different scheduling (once a day for all and twice a day for some) with failure recovery like behavior.
  starlake.sh extract-data --config my-config --output-dir $PWD/output --includeSchemas aSchema
         --includeTables table1RefreshedTwiceADay,table2RefreshedTwiceADay --ifExtractedBefore "2023-04-21 12:00:00"
         --clean



## Parameters

Parameter|Cardinality|Description
---|---|---
--mapping:`<value>`|*Optional*|Deprecated. Use config instead
--config:`<value>`|*Required*|Database tables & connection info
--limit:`<value>`|*Optional*|Limit number of records
--numPartitions:`<value>`|*Optional*|parallelism level regarding partitionned tables
--parallelism:`<value>`|*Optional*|parallelism level of the extraction process. By default equals to the available cores: 10
--separator:`<value>`|*Optional*|Column separator
--clean:`<value>`|*Optional*|Clean all files of table only when it is extracted.
--output-dir:`<value>`|*Required*|Where to output csv files
--fullExport:`<value>`|*Optional*|Force full export to all tables
--datePattern:`<value>`|*Optional*|Pattern used to format date during CSV writing
--timestampPattern:`<value>`|*Optional*|Pattern used to format timestamp during CSV writing
--ifExtractedBefore:`<value>`|*Optional*|DateTime to compare with the last beginning extraction dateTime. If it is before that date, extraction is done else skipped.
--cleanOnExtract:`<value>`|*Optional*|Deprecated. Use --clean instead.
--includeSchemas:`schema1,schema2`|*Optional*|Domains to include during extraction.
--excludeSchemas:`schema1,schema2...`|*Optional*|Domains to exclude during extraction. if `include-domains` is defined, this config is ignored.
--includeTables:`table1,table2,table3...`|*Optional*|Schemas to include during extraction.
--excludeTables:`table1,table2,table3...`|*Optional*|Schemas to exclude during extraction. if `include-schemas` is defined, this config is ignored.
--:`<value>`|*Optional*|

