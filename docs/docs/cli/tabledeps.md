---
sidebar_position: 160
title: table-dependencies
---


## Synopsis

**starlake table-dependencies [options]**

## Description
Generate GraphViz files from Domain / Schema YAML files

## Parameters

Parameter|Cardinality|Description
---|---|---
--output:`<value>`|*Optional*|Where to save the generated dot file ? Output to the console by default
--all-attrs:`<value>`|*Optional*|Should we include all attributes in the dot file or only the primary and foreign keys ? true by default
--reload:`<value>`|*Optional*|Should we reload the domains first ?
--svg:`<value>`|*Optional*|Should we generate SVG files ?
--png:`<value>`|*Optional*|Should we generate PNG files ?
--related:`<value>`|*Optional*|Should we include only entities with relations to others ? false by default
--tables:`<value>`|*Optional*|Which tables should we include in the dot file ?
--all:`<value>`|*Optional*|Include all tables in the dot file ? None by default

