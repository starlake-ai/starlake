source ../env.sh

GCP_BUCKET_NAME="${GCP_BUCKET_NAME:-starlake-app}"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-starlake-325712}"
GCP_SA_JSON_PATH="${GCP_SA_JSON_PATH:-$HOME/.gcloud/keys/starlake-$USER.json}"

export GOOGLE_APPLICATION_CREDENTIALS="$GCP_SA_JSON_PATH"

if [[ -z "$GCP_BUCKET_NAME" ]]; then
    echo "Must provide GCP_BUCKET_NAME in environment" 1>&2
    exit 1
fi

if [[ -z "$GCP_PROJECT_ID" ]]; then
    echo "Must provide GCP_PROJECT_ID in environment" 1>&2
    exit 1
fi

if [[ -z "$GCP_SA_JSON_PATH" ]]; then
    echo "Must provide GCP_SA_JSON_PATH in environment" 1>&2
    exit 1
fi

export COMET_ENV=BQ
export SPARK_DRIVER_MEMORY=4G
export COMET_FS="gs://$GCP_BUCKET_NAME"
export COMET_ROOT="/tmp/quickstart"
export COMET_METRICS_ACTIVE=true
export COMET_ASSERTIONS_ACTIVE=true
export COMET_AUDIT_SINK_TYPE=BigQuerySink
export COMET_SINK_TO_FILE=false
export COMET_ANALYZE=false
export COMET_HIVE=false
export COMET_GROUPED=false
export COMET_MAIN=com.ebiznext.comet.job.Main
export COMET_METRICS_PATH="/tmp/quickstart/metrics/{domain}"
export SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"
export SPARK_CONF_OPTIONS="--conf spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=$GCP_SA_JSON_PATH \
                           --conf spark.yarn.appMasterEnv.GOOGLE_APPLICATION_CREDENTIALS=$GCP_SA_JSON_PATH"

if test -f "../bin/$SPARK_DIR_NAME/jars/spark-bigquery-latest_2.12.jar"; then
  echo "spark-bigquery-latest.jar found"
else
  gsutil cp gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar ../bin/$SPARK_DIR_NAME/jars/
fi
