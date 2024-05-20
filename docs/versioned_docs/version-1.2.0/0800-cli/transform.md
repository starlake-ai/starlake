---
sidebar_position: 310
title: transform
---


## Synopsis

**starlake transform [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--name:`<value>`|*Required*|Task Name
--compile:`<value>`|*Optional*|Return final query only
--interactive:`<value>`|*Optional*|Run query without sinking the result
--reload:`<value>`|*Optional*|Reload YAML  files. Used in server mode
--truncate:`<value>`|*Optional*|Force table to be truncated before insert. Default value is false
--recursive:`<value>`|*Optional*|Execute all dependencies recursively. Default value is false
--test:`<value>`|*Optional*|Should we run this transform as a test ? Default value is false
--accessToken:`<value>`|*Optional*|Access token to use for authentication
--options:`k1=v1,k2=v2...`|*Optional, Unbounded*|Job arguments to be used as substitutions

