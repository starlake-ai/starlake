export SL_BIN_DIR="$(pwd)/../../distrib"
export SL_ENV=SNOWFLAKE
export SPARK_DRIVER_MEMORY=4G
export SL_ROOT="$(pwd)/quickstart"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SL_MAIN=ai.starlake.job.Main
export SF_TIMEZONE=TIMESTAMP_LTZ

if [[ -z "$SNOWFLAKE_ACCOUNT" ]] || [[ -z "$SNOWFLAKE_USER" ]] || [[ -z "$SNOWFLAKE_PASSWORD" ]] || [[ -z "$SNOWFLAKE_WAREHOUSE" ]] || [[ -z "$SNOWFLAKE_DB" ]]; then
    echo "Must provide SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DB in environment" 1>&2
    exit 1
fi

