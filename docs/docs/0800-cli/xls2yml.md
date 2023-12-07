---
sidebar_position: 210
title: xls2yml
---


## Synopsis

**starlake xls2yml [options]**

## Description


## Parameters

Parameter|Cardinality|Description
---|---|---
--files:`<value>`|*Required*|List of Excel files describing domains & schemas or jobs
--iamPolicyTagsFile:`<value>`|*Optional*|If true generate IAM PolicyTags YML
--outputDir:`<value>`|*Optional*|Path for saving the resulting YAML file(s). Starlake domains path is used by default.
--policyFile:`<value>`|*Optional*|Optional File for centralising ACL & RLS definition.
--job:`<value>`|*Optional*|If true generate YML for a Job.

