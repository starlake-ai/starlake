---
sidebar_position: 80
title: cnxload
---


## Synopsis

**starlake cnxload [options]**

## Description

Load parquet file into JDBC Table.


## Parameters

Parameter|Cardinality|Description
---|---|---
--source_file:`<value>`|*Required*|Full Path to source file
--output_table:`<value>`|*Required*|JDBC Output Table
--options:`<value>`|*Optional*|Connection options eq for jdbc : driver, user, password, url, partitions, batchSize
--create_disposition:`<value>`|*Optional*|Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition
--write_disposition:`<value>`|*Optional*|Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition

