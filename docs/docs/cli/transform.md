---
sidebar_position: 120
title: transform | job
---


## Synopsis

**comet transform | job [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--name:`<value>`|*Required*|Job Name
--views:`<value>`|*Optional*|view1,view2 ...<br />If present only the request present in these views statements are run. Useful for unit testing<br />
--views-dir:`<value>`|*Optional*|Where to store the result of the query in JSON
--views-count:`<value>`|*Optional*|Max number of rows to retrieve. Negative value means the maximum value 2147483647
--options:`k1=v1,k2=v2...`|*Optional*|Job arguments to be used as substitutions
