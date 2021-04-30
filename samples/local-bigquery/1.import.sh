source ./env.sh
initEnv
awk -v bucket="$GCP_BUCKET_NAME" -v project="$GCP_PROJECT_ID" -v json="$GCP_SA_JSON_PATH" '{gsub("GCP_BUCKET_NAME", bucket);gsub("GCP_PROJECT_ID", project);gsub("GCP_SA_JSON_PATH", json)}1' core-site.xml >../bin/$SPARK_DIR_NAME/conf/core-site.xml
set -x

gsutil rm -r gs://$GCP_BUCKET_NAME/tmp/*
gsutil cp -r quickstart/ gs://$GCP_BUCKET_NAME/tmp/


$COMET_SCRIPT import
