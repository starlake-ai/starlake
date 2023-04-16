

# Application Credentials

## Using JSON Key File
```shell
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/.gloud/keys/starlake-mykey.json

export SL_STORAGE_CONF="fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS,
google.cloud.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE,
google.cloud.auth.service.account.json.keyfile=$GOOGLE_APPLICATION_CREDENTIALS,
fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem,
google.cloud.auth.service.account.enable=true,
fs.default.name=$SL_FS,
fs.defaultFS=$SL_FS"
```

## Using Application Default (aka GOOGLE_APPLICATION_CREDENTIALS)
```shell
export SL_STORAGE_CONF="fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS,
google.cloud.auth.type=APPLICATION_DEFAULT,
fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem,
google.cloud.auth.service.account.enable=true,
fs.default.name=$SL_FS,
fs.defaultFS=$SL_FS"
```

## Using Compûte engine credentials
```shell
export SL_STORAGE_CONF="fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS,
google.cloud.auth.type=COMPUTE_ENGINE,
fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem,
google.cloud.auth.service.account.enable=true,
fs.default.name=$SL_FS,
fs.defaultFS=$SL_FS"
```

## Example running using application default

```shell
GCP_BUCKET_NAME=starlake-app
gsutil -m rm -r gs://$GCP_BUCKET_NAME/mnt/starlake-app/*
gsutil -m cp -r ../quickstart/ gs://$GCP_BUCKET_NAME/mnt/starlake-app/


docker run \
-v $HOME/.gloud/keys:/app/gcloud \
-e GOOGLE_APPLICATION_CREDENTIALS=/app/gcloud/starlake-mykey.json
-e GCP_BUCKET_NAME=starlake-app \
-e GCP_PROJECT_ID=starlake-325712 \
-e SL_STORAGE_CONF="fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS, \
google.cloud.auth.type=APPLICATION_DEFAULT, \
fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem, \
google.cloud.auth.service.account.enable=true, \
fs.default.name=gs://starlake-app, \
fs.defaultFS=gs://starlake-app" \
-e SL_ENV=BQ \
-e SPARK_DRIVER_MEMORY=4G \
-e SL_FS="gs://starlake-app" \
-e SL_ROOT="/mnt/starlake-app/quickstart" \
-e SL_AUDIT_SINK_TYPE=BigQuerySink \
-e SL_SINK_TO_FILE=false \
-it  --entrypoint bash starlake-m1
```
