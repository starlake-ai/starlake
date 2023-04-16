source ../env.sh

export SL_ENV=FS
export SPARK_DRIVER_MEMORY=4G
export SL_FS=file://
export SL_ROOT="$(pwd)/quickstart"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SL_SINK_TO_FILE=true
export SL_ANALYZE=false
export SL_HIVE=false
export SL_GROUPED=false
export SL_METRICS_PATH="/tmp/metrics/{domain}"
export SL_MAIN=ai.starlake.job.Main
export SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"
