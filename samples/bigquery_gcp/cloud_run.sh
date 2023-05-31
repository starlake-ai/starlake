gcloud run jobs create bigquery_gcp   \
      --image MY_DOCKER_IMAGE   \
      --region europe-west1   --platform managed   --concurrency 1   --timeout 900   --args="import"   \
      --cpu=8   --max-retries=1   -memory=32Gi   --parallelism=1   --execute-now   \
      --set-env-vars=SL_STORAGE_CONF="fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS,google.cloud.auth.type=APPLICATION_DEFAULT,fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem,google.cloud.auth.service.account.enable=true,fs.default.name=gs://MY_BUCKET,fs.defaultFS=gs://MY_BUCKET",GCP_BUCKET_NAME=MY_BUCKET,GCP_PROJECT_ID=MY_GCP_PROJECT_ID,SL_ENV=BQ,SL_FS=gs://MY_BUCKET,SL_ROOT=/mnt/starlake-app/quickstart,SL_AUDIT_SINK_TYPE=BigQuerySink,SL_SINK_TO_FILE=false,SPARK_LOCAL_HOSTNAME=127.0.0.1
