---
sidebar_position: 110
title: extract
---


## Synopsis

**starlake extract [options]**

## Description

For domain extraction, the schemas should at least, specify :
- a table name (schemas.name)
- a file pattern (schemas.pattern) which is used as the export file base name
- a write mode (schemas.metadata.write): APPEND or OVERWRITE
- a delta column (schemas.merge.timestamp) if in APPEND mode : the default column which is used to determine new rows for each exports
- the columns to extract (schemas.attributes.name*)

You also have to provide a Mustache (http://mustache.github.io/mustache.5.html) template file.

In there you'll write your extraction export process (sqlplus for Oracle, pgsql for PostgreSQL as an example).
In that template you can use the following parameters:
- table_name  -> the table to export
- delimiter   -> the resulting dsv file delimiter
- columns     -> the columns to export
   columns is a Mustache map, it gives you access, for each column, to:
    - name               -> the column name
    - trailing_col_char  -> the separator to append to the column (, if there are more columns to come, "" otherwise)
                            Here is an example how to use it in a template:
````sql
                              SELECT
                              {{#columns}}
                              TO_CHAR({{name}}){{trailing_col_char}}
                              {{/columns}}
                              FROM
                              {{table_name}};
````
 export_file -> the export file name
 full_export -> if the export is a full or delta export (the logic is to be implemented in your script)


## Parameters

Parameter|Cardinality|Description
---|---|---
--script-gen:`<value>`|*Optional*|
--domain:`domain1,domain2 ...`|*Optional*|The domain list for which to generate extract scripts
--job:`job1,job2 ...`|*Optional*|The jobs you want to load. use '*' to load all jobs 
--templateFile:`<value>`|*Required*|Script template file
--scriptsOutputDir:`<value>`|*Required*|Scripts output folder
--deltaColumn:`<value>`|*Optional*|The default date column used to determine new rows to export. Overrides config database-extractor.default-column value.
--scriptsOutputPattern:`<value>`|*Optional*|Default output file pattern name<br />the following variables are allowed.<br />When applied to a domain:<br />  - {{domain}}: domain name<br />  - {{schema}}: Schema name<br />  By default : EXTRACT_{{schema}}.sql<br />When applied to a job:<br />  - {{job}}: job name<br />  By default: {{job}}.py<br />  

