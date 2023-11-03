source ./env.sh

export SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE=dynamic
export SL_MERGE_OPTIMIZE_PARTITION_WRITE=true

$SL_BIN_DIR/starlake.sh watch
