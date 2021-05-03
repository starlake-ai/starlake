source ../env.sh

HDFS_URI=hdfs://localhost:9000

if [[ -z "$HDFS_URI" ]]; then
    echo "Must provide HDFS_URI in environment" 1>&2
    exit 1
fi
export COMET_ENV=FS
export SPARK_DRIVER_MEMORY=4G
export COMET_FS="$HDFS_URI"
export COMET_ROOT="/tmp/quickstart"
export COMET_METRICS_ACTIVE=true
export COMET_ASSERTIONS_ACTIVE=true
export COMET_AUDIT_SINK_TYPE=NoneSink
export COMET_SINK_TO_FILE=true
export COMET_ANALYZE=false
export COMET_HIVE=false
export COMET_GROUPED=false
export COMET_METRICS_PATH="/tmp/quickstart/metrics/{domain}"
export SPARK_CONF_OPTIONS="--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"
#                           --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"


export COMET_SCRIPT="$SPARK_SUBMIT $SPARK_CONF_OPTIONS --class com.ebiznext.comet.job.Main $COMET_JAR_FULL_NAME"

