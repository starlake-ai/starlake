source ./env.sh
set -x
export SPARK_CONF_OPTIONS="--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///tmp/spark-events/ --conf spark.driver.extraJavaOptions=-Dconfig.file=$PWD/application.conf --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"

SPARK_DRIVER_OPTIONS="-Dconfig.file=$PWD/application.conf -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"

DRIVER_JAR="--jars $PWD/../bin/postgresql-42.3.1.jar"

$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS $DRIVER_JAR --class $COMET_MAIN $COMET_JAR_FULL_NAME infer-ddl --datawarehouse postgres --connection postgresql --output /tmp/sql.txt #--apply
