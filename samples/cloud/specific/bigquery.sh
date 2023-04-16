BUCKET_NAME=starlake-app
SL_ROOT=/mnt/quickstart
PROJECT_ID=starlake-325712
LOCATION=europe-west1

gcloud dataproc clusters create cluster-starlake
      --region europe-west1
      --zone europe-west1-b
      --master-machine-type n1-standard-4
      --master-boot-disk-size 500
      --num-workers 2
      --worker-machine-type n1-standard-4
      --worker-boot-disk-size 500
      --image-version 2.0-debian10
      --project starlake-325712
      --properties
        spark-env:SL_AUDIT_SINK_TYPE=BigQuerySink,
        spark-env:SL_ENV=BQ,
        spark-env:SL_FS=gs://$BUCKET_NAME,
        spark-env:SL_MAIN=ai.starlake.job.Main,
        spark-env:SL_ROOT=$SL_ROOT

gcloud dataproc jobs wait job-637ccb65 --project $PROJECT_ID --region $LOCATION

POST /v1/projects/starlake-325712/regions/europe-west1/jobs:submit/
{
  "projectId": "starlake-325712",
  "job": {
    "placement": {},
    "statusHistory": [],
    "reference": {
      "jobId": "job-aacf2cd5",
      "projectId": "starlake-325712"
    },
    "sparkJob": {
      "mainClass": "ai.starlake.job.Main",
      "properties": {},
      "jarFileUris": [
        "gs://starlake-app/star10.jar",
        "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
      ],
      "args": [
        "import"
      ]
    }
  }
}
