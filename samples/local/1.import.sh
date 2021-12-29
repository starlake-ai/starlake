if [[ -z "$STARLAKE_ENV" ]]; then
    echo "Must provide STARLAKE_ENV in environment" 1>&2
    exit 1
fi

if (["LOCAL", "HDFS", "GCP"].indexOf(foo) > -1)

case $STARLAKE_ENV in
    LOCAL|HDFS|GCP) echo "Running  in $STARLAKE_ENV env";;
    *)             echo "$STARLAKE_ENV for STARLAKE_ENV unknown"; exit 1;;
esac

source ./env.${STARLAKE_ENV}.sh

set -x

#SPARK_DRIVER_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"
$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --class $COMET_MAIN $COMET_JAR_FULL_NAME import
