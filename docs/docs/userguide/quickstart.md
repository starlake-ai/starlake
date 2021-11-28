---
sidebar_position: 1
---

#Quick Start

## Example
Say we have to ingest customers, orders and sellers into the datalake.
The customers and orders are provided by the "sales" department while
the sellers and locations datasets are provided by the HR department.

The orders dataset contains new, updated and deleted orders.
Once imported, we want the deleted orders to be removed from the dataset and
we want to keep only the last update of each order.


The `locations` dataset should replace the previous imported `locations` dataset
while all others datasets are just updates of the previous imported ones.

The customers and orders dataset are sent by the "sales" department
as CSV  files. Below is an extract of these files.

``File customers-2018-05-10.psv from "sales" department``

id|signup|contact|birthdate|name1|name2
---|---|---|---|---|---
A009701|2010-01-31 23:04:15|me@home.com|1980-10-14|Donald|Obama
B308629|2016-12-01 09:56:02|you@land.com|1980-10-14|Barack|Trump

``File orders-2018-05-10.csv from the "sales" department``

order_id|customer_id|amount|seller_id
---|---|---|---
12345|A009701|123.65|AQZERD
56432|B308629|23.8|AQZERD

:::note 

Before sending the files, the "sales" department zip all its files
into a single compressed files and put them in the folder /mnt/incoming/sales of the landing area.

:::

The `sellers` dataset is sent as JSON array by the HR department.

``File sellers-2018-05-10.json from the HR department``

````json
[
{ "id":"AQZERD", "seller_email":"me@acme.com", "location_id": 1},
{ "id":"TYUEZG", "seller_email":"acme.com","location_id": 2 }

]
````

``File locations-2018-05-10.json from the HR department``

````json
{ "id":1, "address": { "city":"Paris", "stores": ["Store 1", "Store 2", "Store 3"] "country":"France" }}
{ "id":2, "address": { "city":"Berlin", "country":"Germany" }}
````



:::note

the HR department does not zip its files. It simply copy them into the
folder /mnt/incoming/hr of the landing area.

:::


:::caution

We intentionnally set an invalid email for the second seller.

:::


## Build & Run
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



### Build it
Clone the project, install sbt 1.5+ and run ``sbt clean assembly``. This will create the assembly in the ``target/scala-2.12`` directory
or simply download the assembly artefact from [Maven Central](https://maven-badges.herokuapp.com/maven-central/ai.starlake/starlake-spark3_2.12)


### Run it
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


