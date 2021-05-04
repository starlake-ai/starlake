---
sidebar_position: 30
title: Transform
---

## Job

A job is a set of transform tasks executed using the specified engine.
````scala
name: String
````

*Required*. Job logical name.

````scala
tasks: List[Task]
````


*Required*. List of transform tasks to execute.

````scala
area : String
````
*Required*. Area where the data is located.

- When using the BigQuery engine, teh area corresponds to the dataset name we will be working on in this job.
- When using the Spark engine, this is folder where the data should be store. Default value is "business"

````scala
format: String
````

*Optional*. output file format when using Spark engine. Ingored for BigQuery. Default value is "parquet".

````scala
coalesce: Boolean
````
*Optional*. When outputting files, should we coalesce it to a single file. Useful when CSV is the output format.

````scala
udf : String
````
*Optional*.

- Register UDFs written in this JVM class when using Spark engine
- Register UDFs stored at this location when using BigQuery engine

````scala
views : Map[String,String]
````
*Optional*. Create temporary views using where the key is the view name and the map the SQL request corresponding to this view using the SQL engine supported syntax.

````scala
engine : String
````
*Optional*. SPARK or BQ. Default value is SPARK.


## Task
Task executed in the context of a job. Each task is executed in its own session.

````scala
sql: String
````
Main SQL request to exexute (do not forget to prefix table names with the database name to avoid conflicts)

````scala
domain: String
````
Output domain in output Area (Will be the Database name in Hive or Dataset in BigQuery)

````scala
dataset: String
````
Dataset Name in output Area (Will be the Table name in Hive & BigQuery)

````scala
write: String
````
Append to or overwrite existing data

````scala
area: String
````
Target Area where domain / dataset will be stored.

````scala
partition: List[String]
````
List of columns used for partitioning the outtput.

````scala
presql: List[String]
````
List of SQL requests to executed before the main SQL request is run

````scala
postsql: List[String]
````
List of SQL requests to executed after the main SQL request is run

````scala
sink: Sink
````
Where to sink the data

````scala
rls: List[RowLevelSecurity]
````
Row level security policy to apply too the output data.

## Partitioning

## Clustering


https://deepsense.ai/optimize-spark-with-distribute-by-and-cluster-by/

## Views

