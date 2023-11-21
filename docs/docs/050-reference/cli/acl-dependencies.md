---
sidebar_position: 10
title: acl-dependencies
---


## Synopsis

**starlake acl-dependencies [options]**

## Description
Generate GraphViz files from Domain / Schema YAML files

## Parameters

Parameter|Cardinality|Description
---|---|---
--output:`<value>`|*Optional*|Where to save the generated dot file ? Output to the console by default
--grantees:`<value>`|*Optional*|Which users should we include in the dot file ? All by default
--reload:`<value>`|*Optional*|Should we reload the domains first ?
--svg:`<value>`|*Optional*|Should we generate SVG files ?
--png:`<value>`|*Optional*|Should we generate PNG files ?
--tables:`<value>`|*Optional*|Which tables should we include in the dot file ? All by default
--all:`<value>`|*Optional*|Include all ACL in the dot file ? None by default

