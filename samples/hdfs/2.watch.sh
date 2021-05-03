source ./env.sh
set -x

awk -v hdfs="$HDFS_URI" '{gsub("HDFS_URI", hdfs)}1' hdfs-site.xml >../bin/$SPARK_DIR_NAME/conf/hdfs-site.xml

$COMET_SCRIPT watch
