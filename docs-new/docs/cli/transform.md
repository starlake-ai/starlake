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
--name:`<value>`|*Required*|Task Name
--compile:`<value>`|*Optional*|Return final query only
--interactive:`<value>`|*Optional*|Run query without sinking the result
--reload:`<value>`|*Optional*|Reload YAML  files. Used in server mode
--drop:`<value>`|*Optional*|Force target table drop before insert. Default value is false
--recursive:`<value>`|*Optional*|Execute all dependencies recursively. Default value is false
--options:`k1=v1,k2=v2...`|*Optional*|Job arguments to be used as substitutions

