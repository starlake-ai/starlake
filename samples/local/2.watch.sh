source ./env.sh
set -x

export COMET_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE=dynamic
export COMET_MERGE_OPTIMIZE_PARTITION_WRITE=true

SPARK_DRIVER_OPTIONS="-Dconfig.file=$PWD/application.conf  -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"
$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --class $COMET_MAIN $COMET_JAR_FULL_NAME watch
