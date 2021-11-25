---
sidebar_position: 2
---

# Build & Run
The example is located in the folder ``samples/quickstart``.
We will ingest the following files:

From the sales department, customers and orders in delimiter separated files :
- customers and orders are appended to the previous imported data
- new orders are added
- updated orders replace existing ones
- and some orders may even be deleted when marked as such in the input dataset

From the HR department, sellers and locations in json files :
- sellers are imported in a cumulative way while locations are imported as full content and overwrite the existing locations dataset
- sellers are loaded as an array of json objects
- locations are received in JSON format



## Build it
Clone the project, install sbt 1.5+ and run ``sbt clean assembly``. This will create the assembly in the ``target/scala-2.12`` directory
or simply download the assembly artefact from [Maven Central](https://maven-badges.herokuapp.com/maven-central/ai.starlake/starlake-spark3_2.12)


## Run it
To run the quickstart on a local filesystem, simply copy the content of the quickstart directory to your /tmp directory.
This will create the ``/tmp/metadata`` and the ``/tmp/incoming`` folders.

Import the datasets into the cluster using spark-submit :

````shell
$SPARK_HOME/bin/spark-submit target/scala-2.12/starlake-spark3_2.12-VERSION-assembly.jar import
````

This will put the datasets in the ``/tmp/datasets/pending/`` folder. In real life, this will be a HDFS or Cloud Storage folder.

Run the ingestion process as follows :

````shell
$SPARK_HOME/bin/spark-submit target/scala-2.12/starlake-spark3_2.12-VERSION-assembly.jar watch
````


This will ingest the four datasets of the two domains (hr & sales) and store them as parquet files into the folders:
- /tmp/datasets/accepted for valid records
- /tmp/datasets/rejected for invalid records
- /tmp/datasets/unresolved for unrecognized files


When run on top of HDFS or any cloud datawarehouse, these datasets are also available as tables.


