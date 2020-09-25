.. _job_concept:

Job
##########################################

A job is a set of transform tasks executed using the specified engine.

.. option:: name: String

*Required*. Job logical name.

.. option:: tasks: List[Task]

*Required*. List of transform tasks to execute.

.. option:: area : String

*Required*. Area where the data is located.

- When using the BigQuery engine, teh area corresponds to the dataset name we will be working on in this job.
- When using the Spark engine, this is folder where the data should be store. Default value is "business"

.. option:: format: String

*Optional*. output file format when using Spark engine. Ingored for BigQuery. Default value is "parquet".

.. option:: coalesce: Boolean

*Optional*. When outputting files, should we coalesce it to a single file. Useful when CSV is the output format.

.. option:: udf : String

*Optional*.

- Register UDFs written in this JVM class when using Spark engine
- Register UDFs stored at this location when using BigQuery engine

.. option:: views : Map[String,String]

*Optional*. Create temporary views using where the key is the view name and the map the SQL request corresponding to this view using the SQL engine supported syntax.

.. option:: engine : String

*Optional*. SPARK or BQ. Default value is SPARK.

