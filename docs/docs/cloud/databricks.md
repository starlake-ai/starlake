---
sidebar_position: 3
title: Databricks on any cloud
---


## Cluster Setup
When running on top of Databricks, you can make complete abstraction of the Cloud provider.
You juste need to setup a Databricks cluster that will make use of the compute and storage resources provided by
the underlying cloud provider. Follow the steps below to run starlake on top of Databricks:

- Create a service account
- Create a Databricks cluster
- Mount the Databricks File System
- Create a Starlake job
- Ingest your data

:::note

The screenshots below are taken from a Databricks cluster running on Google Cloud but are also valid for Azure and AWS.

:::

### Create a service account
Crate a bucket and name it for example `starlake-app`. This bucket will have the following purposes:
- Store Starlake jars
- Store Starlake metadata 
- Store parquet files after ingestion

Create a service account and assign it the Storage Admin role. 

![Create Service Account]( /img/databricks/create-service-account.png "create service account")

### Create a Databricks Cluster

In a Databricks Workspace, create a cluster and set the value of the `Service Account` field name to the service account you just create in the step above.
![Create Databricks Cluster]( /img/databricks/cluster.png "create service account")

In the `Advanced Settings / Spark Config`  page set the variables below:

![Spark config]( /img/databricks/spark-config.png "Spark config")

Spark Config entry|Value|Description
---|---|---
spark.hadoop.google.cloud.auth.service.account.enable|true| Enable service account auth
spark.hadoop.fs.gs.auth.service.account.email|me@dummy.iam.gserviceaccount.com|Service account name
spark.hadoop.fs.gs.project.id|my-project-id-123456|Project id
spark.hadoop.fs.gs.auth.service.account.private.key|-----BEGIN PRIVATE KEY----- YOUR PRIVATE KEY GOES HERE-----END PRIVATE KEY-----|Private key as defined in your JSON file by the attribute `private_key`
spark.hadoop.fs.gs.auth.service.account.private.key.id|df728e47e5e6c14402fafe6d39a3b8792a9967c7|Private key as defined in your JSON file by the attribute `private_key_id`

In the `Advanced Settings / Environment variables`  section set the variables below:

![Environnement variables]( /img/databricks/env-vars.png "Environnement variables")

Environnement variable |Value|Description
---|---|---
COMET_METRICS_ACTIVE|true|Should we compute metrics set on individuals table columns at ingestion time
COMET_ROOT|/mnt/starlake-app/tmp/quickstart|This is a DBFS mounted directory (see below). It should reference the base directory where your starlake metadata is located
COMET_AUDIT_SINK_TYPE|BigQuerySink|Where to save audit logs. Here we decide to save it in BigQuery. Tos ave it as a hive table or file on the cloud storage, set it to FsSink
COMET_FS|dbfs://|Filesystem. Always set it to DBFS in Databricks.
COMET_HIVE|true|Should we store the resulting parquet files as Databricks tables ?
TEMPORARY_GCS_BUCKET|starlake-app|Bucket name where Google Cloud API store temporary files when saving data to BigQuery


### Mount DBFS

Databricks virtualize the underlying filesystem through DBFS. We first need to enable it in `Admin COnsole / Workspace Settings` page:


![Enable DBFS]( /img/databricks/advanced-settings.png "Enable DBFS")

We now inside a `notebook` mount the cloud storage bucket created above and referenced in the cluster environment variables into DBFS:


````python
bucket_name = "starlake-app"
mount_name = "starlake-app"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))
````

The `gs://starlake-app` bucket is now accessible as a folder from Spark as the folder `dbfs:/mnt/starlake-app`

### Create a Starlake job

To create a starlake job, you first upload the starlake uber jar and the jackson yaml (de)serializer into the gs://starlake-app folder.

![bucket details]( /img/databricks/bucket.png "bucket details")

The version of the `jackson-dataformat-yaml`depends follows the version 
of the others `jackson` components referenced by the databricks runtime

Create tasks and reference the two jars you uploaded to the cloud storage bucket and now visible to databricks through the `dbfs:/mnt/starlake-app` mount

- The first task (`import`) will copy the files matching the expected patterns into the pending directory for ingestion by starlake 

![starlake import]( /img/databricks/import-task.png "starlake import")

- The second task (`watch`) will run the starlake ingestion job store the result as parquet files in the gs://starlake-app bucket.

![starlake watch]( /img/databricks/watch-task.png "starlake watch")

### Ingest your data
Start the `import`task first and then the `watch` task. The execution logs are available through the `runs`tab:

![tasks runs]( /img/databricks/runs.png "tasks runs")


Since we set the `COMET_HIVE=true` environnment variable, ingested data are also available as tables.
![starlake watch]( /img/databricks/database.png "starlake watch")

The audit log for the above tasks will be stored in a BigQuery table since we set `COMET_AUDIT_SINK_TYPE=BigQuerySink` environnment variable 




