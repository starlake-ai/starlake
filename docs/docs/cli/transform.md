---
sidebar_position: 180
title: transform
---


## Synopsis

**starlake transform [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--name:`<value>`|*Required*|Job Name
--compile:`<value>`|*Optional*|Return final query only
--interactive:`<value>`|*Optional*|Run query without 
--no-sink:`<value>`|*Optional*|Just run the query and return rows
--reload:`<value>`|*Optional*|Just run the query and return rows
--views-dir:`<value>`|*Optional*|Useful for testing. Where to store the result of the query in JSON
--authInfo:`<value>`|*Optional*|Auth Info.  Google Cloud use: gcpProjectId and gcpSAJsonKey
--views-count:`<value>`|*Optional*|Useful for testing. Max number of rows to retrieve. Negative value means the maximum value 2147483647
--drop:`<value>`|*Optional*|Force target table drop before insert. Default value is false
--options:`k1=v1,k2=v2...`|*Optional*|Job arguments to be used as substitutions

