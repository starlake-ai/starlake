---
sidebar_position: 100
title: yml2ddl
---


## Synopsis

**starlake yml2ddl [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--datawarehouse:`<value>`|*Required*|target datawarehouse name (ddl mapping key in types.yml
--connection:`<value>`|*Optional*|JDBC connection name with at least read write on database schema
--output:`<value>`|*Optional*|Where to output the generated files. ./$datawarehouse/ by default
--catalog:`<value>`|*Optional*|Database Catalog if any
--domain:`<value>`|*Optional*|Domain to create DDL for. All by default
--schemas:`<value>`|*Optional*|List of schemas to generate DDL for. All by default
--apply:`<value>`|*Optional*|Does the file contain a header (For CSV files only)
--parallelism:`<value>`|*Optional*|parallelism level. By default equals to the available cores: 10

