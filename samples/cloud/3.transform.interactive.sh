source ./env.sh
set -x

$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS  --class $COMET_MAIN $COMET_JAR_FULL_NAME transform --name kpi --interactive table
