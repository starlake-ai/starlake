---
sidebar_position: 30
title: esload
---


## Synopsis

**starlake esload [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--timestamp:`<value>`|*Optional*|Elasticsearch index timestamp suffix as in {@timestamp|yyyy.MM.dd}
--id:`<value>`|*Optional*|Elasticsearch Document Id
--mapping:`<value>`|*Optional*|Path to Elasticsearch Mapping File
--domain:`<value>`|*Required*|Domain Name
--schema:`<value>`|*Required*|Schema Name
--format:`<value>`|*Required*|Dataset input file : parquet, json or json-array
--dataset:`<value>`|*Optional*|Input dataset path
--conf:`es.batch.size.entries=1000, es.batch.size.bytes=1mb...`|*Optional*|esSpark configuration options. See https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html

