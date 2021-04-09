*******
Example
*******


Say we have to ingest customers, orders and sellers into the datalake.
The customers and orders are provided by the "sales" department while
the sellers and locations datasets are provided by the HR department.

The orders dataset contains new, updated and deleted orders.
Once imported, we want the deleted orders to be removed from the dataset and
we want to keep only the last update of each order.


The locations dataset should replace the previous imported locations dataset
while all others datasets are just updates of the previous imported ones.

The customers and orders dataset are sent by the "sales" department
as CSV  files. Below is an extract of these files.

``File customers-2018-05-10.psv from "sales" department``

.. code-block:: text

 id|signup|contact|birthdate|name1|name2
 A009701|2010-01-31 23:04:15|me@home.com|1980-10-14|Donald|Obama
 B308629|2016-12-01 09:56:02|you@land.com|1980-10-14|Barack|Trump

``File orders-2018-05-10.csv from the "sales" department``

.. code-block:: text


 order_id,customer_id,amount,seller_id
 12345,A009701,123.65,AQZERD
 56432,B308629,23.8,AQZERD

.. note::
 Before sending the files, the "sales" department zip all its files
 into a single compressed files and put them in the folder /mnt/incoming/sales of the landing area.

The sellers dataset is sent as JSON array by the HR department.

``File sellers-2018-05-10.json from the HR department``

.. code-block:: json

 [
     { "id":"AQZERD", "seller_email":"me@acme.com", "location_id": 1},
     { "id":"TYUEZG", "seller_email":"acme.com","location_id": 2 }

 ]

``File locations-2018-05-10.json from the HR department``

.. code-block:: json

    { "id":1, "address": { "city":"Paris", "stores": ["Store 1", "Store 2", "Store 3"] "country":"France" }}
    { "id":2, "address": { "city":"Berlin", "country":"Germany" }}




.. note::
 the HR department does not zip its files. It simply copy them into the
 folder /mnt/incoming/hr of the landing area.

.. warning::
 We intentionnally set an invalid email for the second seller.

