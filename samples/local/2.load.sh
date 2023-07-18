if [[ -z "$SL_ENV" ]]; then
    echo "SL_ENV not provided using default value LOCAL" 1>&2
fi

export SL_ENV="${SL_ENV:-LOCAL}"


case $SL_ENV in
    LOCAL|HDFS|BQ|SNOWFLAKE) echo "Running  in $SL_ENV env";;
    *)             echo "$SL_ENV for SL_ENV unknown"; exit 1;;
esac

# shellcheck disable=SC1090
source ./env."${SL_ENV}".sh

export SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE=dynamic
export SL_MERGE_OPTIMIZE_PARTITION_WRITE=true

$SL_BIN_DIR/starlake.sh watch
