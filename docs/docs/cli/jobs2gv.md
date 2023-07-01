---
sidebar_position: 120
title: jobs2gv
---


## Synopsis

**starlake jobs2gv [options]**

## Description
Generate GraphViz files from Job YAML files

## Parameters

Parameter|Cardinality|Description
---|---|---
--output-dir:`<value>`|*Optional*|Where to save the generated dot file ? Output to the console by default
--jobs:`<value>`|*Optional*|Compute dependencies of this job only. If not specified, compute all jobs.
--reload:`<value>`|*Optional*|Should we reload the domains first ?
--verbose:`<value>`|*Optional*|Should we generate one graph per job ?
--objects:`<value>`|*Optional*|comma separated list of objects to display: task, table, view, unknown

