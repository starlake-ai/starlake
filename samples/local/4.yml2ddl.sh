if [[ -z "$STARLAKE_ENV" ]]; then
    echo "Must provide STARLAKE_ENV in environment" 1>&2
    exit 1
fi

case $STARLAKE_ENV in
    LOCAL|HDFS|GCP) echo "Running  in $STARLAKE_ENV env";;
    *)             echo "$STARLAKE_ENV for STARLAKE_ENV unknown"; exit 1;;
esac

# shellcheck disable=SC1090
source ./env."${STARLAKE_ENV}".sh

set -x
export SPARK_CONF_OPTIONS="--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///tmp/spark-events/ --conf spark.driver.extraJavaOptions=-Dconfig.file=$PWD/application.conf --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"

SPARK_DRIVER_OPTIONS="-Dconfig.file=$PWD/application.conf -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"

DRIVER_JAR="--jars $PWD/../bin/postgresql-42.3.1.jar"

$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS $DRIVER_JAR --class $COMET_MAIN $COMET_JAR_FULL_NAME infer-ddl --datawarehouse postgres --connection postgresql --output /tmp/sql.txt #--apply
