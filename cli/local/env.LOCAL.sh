source ../env.sh


export SPARK_DRIVER_MEMORY=4G
export COMET_FS=file://
#export COMET_ROOT="$(pwd)/quickstart"
export COMET_METRICS_ACTIVE=false
export COMET_ASSERTIONS_ACTIVE=false
export COMET_SINK_TO_FILE=true
export COMET_ANALYZE=false
export COMET_HIVE=false
export COMET_GROUPED=false
export COMET_MAIN=ai.starlake.job.Main

export SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties"
#export SPARK_DRIVER_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties"

initEnv > /dev/null 2>&1
