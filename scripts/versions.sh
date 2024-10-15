#!/bin/bash
set -e

while read line; do
  export $line
done < .versions

FIRST_LINE=$(head -n 1 version.sbt)
export SL_VERSION=$(echo "$FIRST_LINE" | sed -E 's/.*version := "([0-9]+\.[0-9]+\.?.*)"/\1/')
export SL_MAJOR_MINOR_VERSION=$(echo "${SL_VERSION}" | cut -d'.' -f1-2)
echo "SL_MAJOR_MINOR_VERSION=${SL_MAJOR_MINOR_VERSION}"
export REGISTRY_IMAGE=starlakeai/starlake
echo "REGISTRY_IMAGE=${REGISTRY_IMAGE}"
export REGISTRY_IMAGE_LATEST=${REGISTRY_IMAGE}:${SL_VERSION:-latest}
echo "REGISTRY_IMAGE_LATEST=${REGISTRY_IMAGE_LATEST}"
export BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
export VCS_REF=$(git rev-parse --short HEAD)
