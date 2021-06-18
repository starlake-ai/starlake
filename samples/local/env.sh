source ../env.sh

export COMET_ENV=FS
export SPARK_DRIVER_MEMORY=4G
export COMET_FS=file://
export COMET_ROOT="$(PWD)/quickstart"
export COMET_METRICS_ACTIVE=true
export COMET_ASSERTIONS_ACTIVE=true
export COMET_SINK_TO_FILE=true
export COMET_ANALYZE=false
export COMET_HIVE=false
export COMET_GROUPED=false
export COMET_METRICS_PATH="/tmp/metrics/{domain}"
export COMET_MAIN=com.ebiznext.comet.job.Main
export SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"
