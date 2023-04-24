GCP_BUCKET_NAME="${GCP_BUCKET_NAME:-starlake-app}"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-starlake-325712}"

export GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS:-$HOME/.gcloud/keys/starlake-$USER.json}"


if [[ -z "$GCP_BUCKET_NAME" ]]; then
    echo "Must provide GCP_BUCKET_NAME in environment" 1>&2
    exit 1
fi

if [[ -z "$GCP_PROJECT_ID" ]]; then
    echo "Must provide GCP_PROJECT_ID in environment" 1>&2
    exit 1
fi

if [[ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]]; then
    echo "Must provide GOOGLE_APPLICATION_CREDENTIALS in environment" 1>&2
    exit 1
fi

export SL_BIN_DIR="$PWD/../../distrib"
export SL_ENV=BQ
export SPARK_DRIVER_MEMORY=4G
export SL_FS="gs://$GCP_BUCKET_NAME"
export SL_ROOT="/mnt/starlake-app/quickstart"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SL_AUDIT_SINK_TYPE=BigQuerySink
export SL_SINK_TO_FILE=false
export SL_ANALYZE=false
export SL_HIVE=false
export SL_GROUPED=false
export SL_MAIN=ai.starlake.job.Main
export SL_METRICS_PATH="/mnt/starlake-app/quickstart/metrics/{domain}"
