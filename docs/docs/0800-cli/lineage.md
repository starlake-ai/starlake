---
sidebar_position: 210
title: lineage
---


## Synopsis

**starlake lineage [options]**

## Description
Generate Task dependencies graph

## Parameters

Parameter|Cardinality|Description
---|---|---
--output:`<value>`|*Optional*|Where to save the generated dot file ? Output to the console by default
--task:`<value>`|*Optional*|Compute dependencies of these tasks only. If not specified, compute all jobs.
--reload:`<value>`|*Optional*|Should we reload the domains first ?
--viz:`<value>`|*Optional*|Should we generate a dot file ?
--svg:`<value>`|*Optional*|Should we generate SVG files ?
--png:`<value>`|*Optional*|Should we generate PNG files ?
--print:`<value>`|*Optional*|Print dependencies as text
--objects:`<value>`|*Optional*|comma separated list of objects to display: task, table, view, unknown
--all:`<value>`|*Optional*|Include all tasks  in the dot file ? None by default

