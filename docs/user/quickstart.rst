***********
Quick Start
***********

The example in the folder ``src/test/resources/quickstart`` import into the cluster teh following files :

From the sales departement, customers and orders in delimiter separated files :
 - customers and orders are appended to the previous imported data
 - new orders are added,
 - updated orders replace existing ones
 - and some orders may even be delted whenmarked as such in the input dataset

From the HR departement, sellers and locations in json files :
 - sellers are imported in a cumulative way while locations are imported as full content and overwrite the existing locations dataset
 - locations are loaded as an array of json objects
 - sellers are received in JSONL format.



Build it
########

Clone the project, install sbt 1.0+ and run ``sbt clean assembly``. This will create the following assembly
in the ``target/scala-2.11`` directory.



Run it
######

To run the quickstart on a local filesystem, simply copy the content of the quickstart directory to your /tmp directory.
This will create the ``/tmp/metadata`` and the ``/tmp/incoming`` folders.

Import the datasets into the cluster using spark-submit :

.. code-block:: console

   $SPARK_HOME/bin/spark-submit target/scala-2.11/comet-assembly-VERSION.jar import


This will put the datasets in the ``/tmp/datasets/pending/`` folder. In real life, this will be a HDFS folder.

Run the ingestion process as follows :

.. code-block:: console

   $SPARK_HOME/bin/spark-submit target/scala-2.11/comet-assembly-VERSION.jar watch


This will ingest the four datasets of the two domains (hr & sales) and store them as parquet files into the folders:
 - /tmp/datasets/accepted for valid records
 - /tmp/datasets/rekjected for invalid records
 - /tmp/datasets/unresolved for unrecognized files


When run on top of HDFS, these datasets are also available as Hive tables.


