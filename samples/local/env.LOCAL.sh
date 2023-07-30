export SL_BIN_DIR="$(pwd)/../../distrib"
export SPARK_DRIVER_MEMORY=4G
export SL_ROOT="$(pwd)/quickstart"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SL_ANALYZE=false
export SL_HIVE=false
export SL_GROUPED=false
export SL_METRICS_PATH="/tmp/metrics/{domain}"
export SL_MAIN=ai.starlake.job.Main

export SL_ENV=LOCAL
export SL_ENGINE=spark
export SL_CONNECTION_REF=spark
