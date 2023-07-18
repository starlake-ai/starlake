source ../env.sh

HDFS_URI=hdfs://localhost:9000

if [[ -z "$HDFS_URI" ]]; then
    echo "Must provide HDFS_URI in environment" 1>&2
    exit 1
fi

export SL_BIN_DIR="$(pwd)/../../distrib"
export SPARK_DRIVER_MEMORY=4G
export SL_FS="$HDFS_URI"
export SL_ROOT="/tmp/quickstart"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SL_AUDIT_SINK_TYPE=DefaultSink
export SL_SINK_TO_FILE=true
export SL_ANALYZE=false
export SL_HIVE=false
export SL_GROUPED=false
export SL_METRICS_PATH="/tmp/quickstart/metrics/{domain}"
export SL_MAIN=ai.starlake.job.Main
