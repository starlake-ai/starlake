source ./env.sh
gsutil -m rm -r gs://$GCP_BUCKET_NAME/mnt/starlake-app/*
gsutil -m cp -r quickstart/ gs://$GCP_BUCKET_NAME/mnt/starlake-app/
