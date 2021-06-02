source ./env.sh
set -x

awk -v hdfs="$HDFS_URI" '{gsub("HDFS_URI", hdfs)}1' hdfs-site.xml >../bin/$SPARK_DIR_NAME/conf/hdfs-site.xml

hdfs dfs -rm -r -f   $HDFS_URI/tmp/quickstart
hdfs dfs -mkdir /tmp/quickstart
hdfs dfs -put ./quickstart/* $HDFS_URI/tmp/quickstart/

$COMET_SCRIPT import
