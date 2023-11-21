---
sidebar_position: 4
title: Google Cloud Dataproc
---
Two options are available when running on Google Cloud:
- Production Mode: Running the jobs against BigQuery with the Dataproc cluster executing on Google Cloud
- Development Mode: Running the jobs against BigQuery with a Spark Job running on your computer.

In both cases, the metadata are stored in Google CLoud Storage, only the spark job is either running 
on Dataproc or locally on your computer

## Production Mode with Google Dataproc

Follow the steps below to run starlake on Google Cloud:

- Create a service account
- Create a Dataproc cluster
- Create a Starlake job
- Ingest your data

### Create a service account

Create a bucket and name it for example `starlake-app`. This bucket will have the following purposes:
- Store Starlake jars
- Store Starlake metadata
- Crteate datasets and tables in BigQuery. since we are ingesting into BigQuery, no parquet file is store on Cloud Storage.

Create a service account and assign it the Storage Admin and BigQuery Admin roles. Depending on your security configuration, 
you may be required to use lower access rights.  

![Create Service Account]( /img/gcloud/create-service-account.png "create service account")

Copy your starlake project to the `gd://starlake-app/mnt/quickstart` directory using the following script run from 
the [samples/cloud](https://github.com/starlake-ai/starlake/tree/master/samples/cloud) folder:

````bash

gsutil cp -r quickstart/ gs://staralke-app/mnt/starlake-app/

````


### Create a Dataproc cluster

Dataproc is the Google service for running Spark jobs. After enabling the Dataproc API, create a cluster and define 
the environnment variables below:

![Create Dataproc cluster]( /img/gcloud/create-dataproc.png "Create Dataproc cluster")



Environnement variable |Value|Description
---|---|---
SL_ROOT|/mnt/starlake-app|It should reference the base directory where your starlake metadata is located
SL_AUDIT_SINK_TYPE|BigQuerySink|Where to save audit logs. Here we decide to save it in BigQuery. Tos ave it as a hive table or file on the cloud storage, set it to FsSink
SL_FS|gs://starlake-app|Filesystem. Reference the cloud storage bucket where all the files will be located.
TEMPORARY_GCS_BUCKET|starlake-app|Bucket name where Google Cloud API store temporary files when saving data to BigQuery
SL_ENV|BQ|Starlake Env variables. This will instruct Starlake to use the env.`BQ`.sl.yml file located at the root of your project when running comet. The `sink_type` in this file instruct Starlake to save datasets in BigQuery instead of parquet files in Cloud Storage.   

To create the dataproc cluster using the CLI instead, just run the command below:

````bash

gcloud dataproc clusters create cluster-88ea \
      --region europe-west1 \
      --zone europe-west1-b \
      --master-machine-type n1-standard-4 \
      --master-boot-disk-size 500 \
      --num-workers 2 \
      --worker-machine-type n1-standard-4 \
      --worker-boot-disk-size 500 \
      --image-version 2.0-debian10 \
      --project my-starlake-project-id \
      --properties \
        spark-env:SL_AUDIT_SINK_TYPE=BigQuerySink, \
        spark-env:SL_ENV=BQ, \
        spark-env:SL_FS=gs://starlake-app, \
        spark-env:SL_ROOT=/mnt/quickstart, \
        spark-env:TEMPORARY_GCS_BUCKET=starlake-app

````

### Create a Starlake job

Assuming that you copied the starlake assembly to the root for the gs://starlake-app bucket, 
just create using the POST request below or through the user interface.

![Create Dataproc job](/img/gcloud/create-import-job.png "Create Dataproc job")


````bash

POST /v1/projects/my-starlake-project-id/regions/europe-west1/jobs:submit/
{
  "projectId": "my-starlake-project",
  "job": {
    "placement": {},
    "statusHistory": [],
    "reference": {
      "jobId": "job-aacf2cd5",
      "projectId": "my-starlake-project-id"
    },
    "sparkJob": {
      "mainClass": "ai.starlake.job.Main",
      "properties": {},
      "jarFileUris": [
        "gs://starlake-app/starlake-VERSION-assembly.jar",
        "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
      ],
      "args": [
        "import"
      ]
    }
  }
}

````

We create above the import job. To create the watch job, just create a new job and replace `import`by `watch` in 
the `Arguments` field as shown below:

![Watch job](/img/gcloud/create-watch-job.png "Watch job")


### Ingest your data

Start the `import` job first and then the `watch` job. The execution logs are available in the Dataproc UI:

![tasks runs]( /img/gcloud/runs.png "tasks runs")


Since we ingested data into BigQuery, We find it available in BigQuery datasets and tables 
![starlake watch]( /img/gcloud/bigquery.png "starlake watch")

The audit log for the above jobs are available in a BigQuery table since we set the `SL_AUDIT_SINK_TYPE=BigQuerySink` environnment variable.


## Running Locally with Spark (Dev. Mode) 

When describing your data format, you may need to run, for testing purposes, your job locally against the remote GCP Project hosting your BigQuery datasets.
In that case, you need to set the GCP_PROJECT env var and create a custom core-site.xml in your classpath as described below :

````xml
 <configuration>
     <property>
         <name>fs.gs.impl</name>
         <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
     </property>
     <property>
         <name>fs.AbstractFileSystem.gs.impl</name>
         <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
     </property>
     <property>
         <name>fs.gs.project.id</name>
         <value>myproject-1234</value>
     </property>
     <property>
         <name>google.cloud.auth.service.account.enable</name>
         <value>true</value>
     </property>
     <property>
         <name>google.cloud.auth.service.account.json.keyfile</name>
         <value>/Users/me/.gcloud/keys/myproject-1234.json</value>
     </property>
     <property>
         <name>fs.default.name</name>
         <value>gs://startlake-app</value>
     </property>
     <property>
         <name>fs.defaultFS</name>
         <value>gs://startlake-app</value>
     </property>
     <property>
         <name>fs.gs.system.bucket</name>
         <value>startlake-app</value>
     </property>
 </configuration>
````
