---
sidebar_position: 2
title: Load
---

## Domain
Let's say you are willing to import customers and orders from your Sales system.
Sales is therefore the domain and customer & order are your datasets.
- In a DBMS, a Domain would be implemented by a DBMS  schema and a dataset by a DBMS table.
- In BigQuery, the domain name would be the Big Query dataset name and the dataset would be implemented by a Big Query table.

````scala
name: String
````

*Required*. Domain name. Make sure you use a name that may be used as a folder name on the target storage.
- When using HDFS or Cloud Storage,  files once ingested are stored in a sub-directory oof a directory named after the domain name.
- When used with BigQuery, files are ingested and sorted in tables under a dataset named after the domain name.


````scala
directory: String
````

*Required*. Folder on the local filesystem where incoming files are stored.
Typically, this folder will be scanned periodically to move the dataset to the cluster for ingestion.
Files located in this folder are moved to the pending folder for ingestion by the "import" command.


````scala
metadata: Metadata
````

*Optional*. Default Schema metadata.
This metadata is applied to the schemas defined in this domain.
Metadata properties may be redefined at the schema level.
See Metadata Entity for more details.

````scala
schemas: List[Schema]
````

*Required*. List of schemas for each dataset in this domain
A domain ususally contains multiple schemas. Each schema defining how the contents of the input file should be parsed.
See :ref:`schema_concept` for more details.

````scala
comment: String
````

*Optional*. Domain Description (free text)

````scala
extensions: List[String]
````

*Optional*. Recognized filename extensions. json, csv, dsv, psv are recognized by default
Only files with these extensions will be moved to the pending folder.

````scala
ack: String
````
*Optional*.
Ack extension used for each file. ".ack" if not specified.
Files are moved to the pending folder only once a file with the same name as the source file and with this extension is present.
To move a file without requiring an ack file to be present, set explicitly this property to the empty string value "".


## Schema


````scala
name: String
````
*Required*. Schema name, must be unique among all the schemas belonging to the same domain.
Will become a hive table name On Premise or BigQuery table name on GCP.

````scala
pattern: String
````
*Required*. Filename pattern to which this schema must be applied. This may be any Java Regex
This instructs the framework to use this schema to parse any file with a filename that match this pattern.

````scala
attributes: List[Attribute]
````

*Required*. Attributes parsing rules.
See :ref:`attribute_concept` for more details.

````scala
metadata: Metadata
````
*Optional*. Dataset metadata
See :ref:`metadata_concept` for more details.

````scala
comment: String
````
*Optional*. Free text

````scala
presql: String
````

*Optional*. Reserved for future use.

````scala
postsql: String
````

*Optional*. Reserved for future use.

````scala
tags: Set[String]
````
*Optional*. Set of string to attach to this Schema

````scala
rls: List[RowLevelSecurity]
````
*Optional*. Experimental. Row level security to apply to this schema once it is ingested.
This usually execute a set on grants by applying a predicate filter to restrict
access to a subset of the rows in the table.
See :ref:`rls_concept` for more details

## Metadata

Specify Schema properties.
These properties may be specified at the schema or domain level
Any property not specified at the schema level is taken from the
one specified at the domain level or else the default value is returned.

````scala
mode: Enum
````
*Optional*. FILE mode by default. FILE and STREAM are the two accepted values. FILE is currently the only supported mode.

````scala
format: Enum
````
*Optional*. DSV by default. Supported file formats are :
- DSV : Delimiter-separated values file. Delimiter value iss specified in the "separator" field.
- POSITION : FIXED format file where values are located at an exact position in each line.
- SIMPLE_JSON : For optimisation purpose, we differentiate JSON with top level values from JSON with deep level fields. SIMPLE_JSON are JSON files with top level fields only.
- JSON :  Deep JSON file. Use only when your json documents contain subdocuments, otherwise prefer to use SIMPLE_JSON since it is much faster.
- XML : For XML files

````scala
encoding: String
````
*Optional*. UTF-8 if not specified.

````scala
multiline: Boolean
````
*Optional*. Are json objects on a single line or multiple line ? Single by default.  false means single. false also means faster

````scala
array: Boolean
````
*Optional*. Is the json stored as a single object array ? false by default. This means that by default we have on json document per line.

````scala
withHeader: Boolean
````
*Optional*. When the input file is in the DSV file format, does the dataset has a header ? true bu default

````scala
separator: String
````
*Optional*. When the input file is in the DSV file format, yhiss field contains the values delimiter,  ';' by default value may be a multichar string starting from Spark 3

````scala
uote: Char
````
*Optional*. The String quote char, '"' by default

````scala
escape: Char
````
*Optional*. Escaping char '\' by default

````scala
write: Enum
````
*Optional*. Write mode, APPEND by default

````scala
partition: List[String]
````
*Optional*. Partition columns, no partitioning by default

````scala
sink: Sink
````
*Optional*. Should the dataset be indexed to this sink ?
See :ref:`sink_concept` for more details.

````scala
ignore: String
````
*Optional*. Pattern to ignore or UDF to apply to ignore some lines

## Attribute

A field in the schema. For struct fields, the field "attributes" contains all sub attributes

````scala
name: String
````
Attribute name as defined in the source dataset and as received in the file

````scala
type: String
````
Semantic type of the attribute.

````scala
array: Boolean
````
Is it an array ?

````scala
required: Boolean
````
Should this attribute always be present in the source

````scala
privacy:PrivacyLevel
````
Should this attribute be applied a privacy transformation at ingestion time

````scala
comment: String
````
free text for attribute description

````scala
rename: String
````
If present, the attribute is renamed with this name

````scala
metricType:MetricType
````
If present, what kind of stat should be computed for this field

````scala
attributes: List[Attribute]
````
List of sub-attributes (valid for JSON and XML files only)

````scala
position: Position
````
Valid only when file format is POSITION

````scala
default: String
````
Default value for this attribute when it is not present.

````scala
tags:Set[String]
````
Tags associated with this attribute

````scala
trim: Trim
````
Should we trim the attribute value ?

````scala
script: String
````
Scripted field : SQL request on renamed column


## Sink
Once ingested, files may be sinked to BigQuery, Elasticsearch or any JDBC compliant Database.

````scala
type: Enum
````
- JDBC : dataset will be sinked to a JDBC Database. See JdbcSink below
- ES : dataset is indexed into Elasticsearch. See EsSink below
- BQ : Dataset is sinked to BigQuery. See BigQuerySink below
- None: Don't sink. This is the default.

````scala
name: String
````
This optional name is used when the configuration is specified in the application.conf file instead of inline in the YAML file.
This is useful when the same sink parameters are used for different datasets.


### BigQuerySink
When the sink type field is set to BQ, the options below shoiuld be provided.
````scala
location: String
````
Database location (EU, US, ...)

````scala
timestamp: String
````
The timestamp column to use for table partitioning if any. No partitioning by default

````scala
clustering: List[String]
````
List of ordered columns to use for table clustering

````scala
days: Int
````
Number of days before this table is set as expired and deleted. Never by default.

````scala
requirePartitionFilter: Boolean
````
Should be require a partition filter on every request ? No by default.

### EsSink
When the sink *type* field is set to ES, the options below should be provided.
Elasticsearch options are specified in the application.conf file.

````scala
id: String
````
Attribute to use as id of the document. Generated by Elasticseach if not specified.

````scala
timestamp: String
````
Timestamp field format as expeted by Elasticsearch ("{beginTs|yyyy.MM.dd}" for example).


### JdbcSink
When the sink *type* field is set to JDBC, the options below should be provided.

````scala
connection: String
````
The JDBC Connection String. Specific to the target JDBC database

````scala
partitions: Int
````
Number of Spark partitions

````scala
batchsize: Int
````
Batch size of each JDBC bulk insert

## RowLevelSecurity

User / Group and Service accounts rights on a subset of the table.

````scala
name: String
````
*Required*. This Row Level Security unique name.

````scala
predicate: String
````
*Required*. The condition that goes to the WHERE clause and limitt the visible rows.

````scala
grants: List[String]
````
*Required*. user / groups / service accounts to which this security level is applied.
For example: user:me@mycompany.com,group:group@mycompany.com,serviceAccount:mysa@google-accounts.com
