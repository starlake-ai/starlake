if [[ -z "$COMET_ROOT" ]]; then
    echo "COMET_ROOT undefined. set it to the starlake project folder" 1>&2
    exit 2
fi

if [[ -z "$SPARK_DIR" ]]; then
    echo "SPARK_DIR undefined. set it to the Spark folder" 1>&2
    exit 2
fi

if [[ -z "$COMET_BIN" ]]; then
    echo "COMET_BIN undefined. set it to the Starlake path" 1>&2
    exit 2
fi

export SPARK_SUBMIT=$SPARK_DIR/bin/spark-submit


if test -f "$COMET_BIN"; then
    echo "COMET_BIN=$COMET_BIN"
else
  echo "COMET_BIN not defined or does not reference the starlake assembly"
  exit 1
fi

if test -d "$SPARK_DIR"; then
    echo "SPARK_DIR=$SPARK_DIR"
else
  echo "SPARK_DIR  does not reference a valid folder"
  exit 1
fi

if test -f $SPARK_SUBMIT; then
    echo "$SPARK_SUBMIT found in $SPARK_DIR/"
    echo "SUCCESS: environment initialized correctly"
else
  echo "$SPARK_SUBMIT not found !!!"
  echo "FAILURE: Failed to initialize environment"
  exit 2
fi

if [ -z "$COMET_LOGLEVEL" ] || [ "$COMET_LOGLEVEL"  = "Default" ]; then
  cp $SPARK_DIR/conf/log4j.properties.template $SPARK_DIR/conf/log4j.properties 
else 
  cp $SPARK_DIR/conf/log4j.properties.template $SPARK_DIR/conf/log4j.properties
  sed '/log4j.rootCategory/d' $SPARK_DIR/conf/log4j.properties.template > $SPARK_DIR/conf/log4j.properties
  echo "log4j.rootCategory=$COMET_LOGLEVEL, console" >> $SPARK_DIR/conf/log4j.properties
fi

export SPARK_DRIVER_MEMORY=4G
export COMET_FS=file://
export COMET_METRICS_ACTIVE=false
export COMET_ASSERTIONS_ACTIVE=false
export COMET_SINK_TO_FILE=true
export COMET_ANALYZE=false
export COMET_HIVE=false
export COMET_GROUPED=false
export COMET_MAIN=ai.starlake.job.Main

#export SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties"
#export SPARK_DRIVER_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties"

#set -x

$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --class $COMET_MAIN $COMET_BIN $*
