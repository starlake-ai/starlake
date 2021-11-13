source ./env.sh
set -x

gsutil cp  customers-2018-01-02.psv gs://$GCP_BUCKET_NAME/tmp/quickstart/datasets/pending/sales/
gsutil cp  quickstart/metadata/domains/sales.comet.yml gs://$GCP_BUCKET_NAME/tmp/quickstart/metadata/domains/sales.comet.yml

