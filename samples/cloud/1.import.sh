source ./env.sh
awk -v bucket="$GCP_BUCKET_NAME" -v project="$GCP_PROJECT_ID" -v json="$GCP_SA_JSON_PATH" '{gsub("GCP_BUCKET_NAME", bucket);gsub("GCP_PROJECT_ID", project);gsub("GCP_SA_JSON_PATH", json)}1' core-site.xml >../bin/$SPARK_DIR_NAME/conf/core-site.xml
set -x

gsutil -m rm -r gs://$GCP_BUCKET_NAME/mnt/starlake-app/*
gsutil -m cp -r quickstart/ gs://$GCP_BUCKET_NAME/mnt/starlake-app/

#SPARK_DRIVER_OPTIONS="-Dconfig.file=$PWD/application.conf -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties.template"

$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS  --class $COMET_MAIN $COMET_JAR_FULL_NAME import
