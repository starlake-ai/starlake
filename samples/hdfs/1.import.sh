source ./env.sh
set -x

awk -v hdfs="$HDFS_URI" '{gsub("HDFS_URI", hdfs)}1' hdfs-site.xml >../bin/$SPARK_DIR_NAME/conf/hdfs-site.xml

hdfs dfs -rm -r -f   $HDFS_URI/tmp/quickstart
hdfs dfs -mkdir /tmp/quickstart
hdfs dfs -put ./quickstart/* $HDFS_URI/tmp/quickstart/

$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --class $COMET_MAIN $COMET_JAR_FULL_NAME import
