---
sidebar_position: 30
title: bqload
---


## Synopsis

**starlake bqload [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--source_file:`<value>`|*Required*|Full Path to source file
--output_database:`<value>`|*Optional*|Target BigQuery Project
--output_dataset:`<value>`|*Required*|BigQuery Output Dataset
--output_table:`<value>`|*Required*|BigQuery Output Table
--output_partition:`<value>`|*Optional*|BigQuery Partition Field
--require_partition_filter:`<value>`|*Optional*|Require Partition Filter
--output_clustering:`col1,col2...`|*Optional*|BigQuery Clustering Fields
--connectionRef:`<value>`|*Optional*|BigQuery Connector
--source_format:`<value>`|*Optional*|Source Format eq. parquet. This option is ignored, Only parquet source format is supported at this time
--create_disposition:`<value>`|*Optional*|Big Query Create disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/CreateDisposition
--write_disposition:`<value>`|*Optional*|Big Query Write disposition https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition
--row_level_security:`<value>`|*Optional*|value is in the form name,filter,sa:sa@mail.com,user:user@mail.com,group:group@mail.com 

