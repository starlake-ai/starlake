#!/usr/bin/env bash
## sbt-gcp.sh publish
export GOOGLE_APPLICATION_CREDENTIALS="/Users/me/.gcloud/keys/project-factory@ebiz-europe-west1.iam.gserviceaccount.com.json"
export GCS_BUCKET_ARTEFACTS="dev-ebiz-europe-west1-ebiznext-com-artefacts"
COMET_SPARK_VERSION=3.0.0 sbt ++2.12.12 $*
