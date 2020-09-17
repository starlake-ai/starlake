.. _task_concept:

*********************************************
Task
*********************************************

Task executed in the context of a job. Each task is executed in its own session.

.. option:: sql: String

Main SQL request to exexute (do not forget to prefix table names with the database name to avoid conflicts)

.. option:: domain: String

Output domain in output Area (Will be the Database name in Hive or Dataset in BigQuery)

.. option:: dataset: String

Dataset Name in output Area (Will be the Table name in Hive & BigQuery)

.. option:: write: String

Append to or overwrite existing data

.. option:: area: String

Target Area where domain / dataset will be stored.

.. option:: partition: List[String]

List of columns used for partitioning the outtput.

.. option:: presql: List[String]

List of SQL requests to executed before the main SQL request is run

.. option:: postsql: List[String]

List of SQL requests to executed after the main SQL request is run

.. option:: sink: Sink

Where to sink the data

.. option:: rls:RowLevelSecurity

Row level security policy to apply too the output data.
