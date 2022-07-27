#!/usr/bin/env bash
## sbt-gcp.sh publish
export GOOGLE_APPLICATION_CREDENTIALS="/Users/me/.gcloud/keys/starlake-$USER.json"
export GCS_BUCKET_ARTEFACTS="starlake-dev-artefacts"
COMET_SPARK_VERSION=3.0.0 sbt ++2.12.16 $*
