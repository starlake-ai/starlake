---
sidebar_position: 110
title: dependencies
---


## Synopsis

**starlake dependencies [options]**

## Description
Generate Task dependencies graph

## Parameters

Parameter|Cardinality|Description
---|---|---
--output-dir:`<value>`|*Optional*|Where to save the generated dot file ? Output to the console by default
--task:`<value>`|*Optional*|Compute dependencies of this job only. If not specified, compute all jobs.
--reload:`<value>`|*Optional*|Should we reload the domains first ?
--verbose:`<value>`|*Optional*|Should we generate one graph per job ?
--viz:`<value>`|*Optional*|Should we generate one graph per job ?
--print:`<value>`|*Optional*|Print dependencies as text
--objects:`<value>`|*Optional*|comma separated list of objects to display: task, table, view, unknown

