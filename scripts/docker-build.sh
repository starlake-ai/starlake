#!/usr/bin/env bash

set -e
set -u
set -o pipefail

SAVEIFS=$IFS
IFS=$(echo -en "\n\b")

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

function usage() {
    echo "Usage: $0 (-e [environment]) (-d [container-engine]) (-b)"
    echo ""
    echo "-e [environment] : build docker image to [environment], *cloud* by default"
    echo "-d [container-engine]: use the selected container engine, *docker* by default"
    echo "-p : publish the application"
}

function printError() {
    echo -e "${RED}$1${NC}" 1>&2
}

function printInfo() {
    echo -e "${GREEN}$1${NC}" 1>&2
}

function clean() {
    IFS=$SAVEIFS
    exit $1
}

if [[ "$0" != "./scripts/docker-build.sh" ]]; then
    echo "ERROR: start docker-build.sh from the starlake root directory as follows: ./scripts/docker-build.sh"
    clean 1
fi

MACHINE="$(uname -m)"
ENVIRONMENT="cloud"
BUILD="false"
PUBLISH="false"
CONTAINER_ENGINE="docker"

echo Running on ${MACHINE}

while getopts ":e:m:d:bp" opt; do
    case ${opt} in
    e) ENVIRONMENT=${OPTARG} ;;
    m) MACHINE=${OPTARG} ;;
    d) CONTAINER_ENGINE=${OPTARG} ;;
    b) BUILD="true" ;;
    p) PUBLISH="true" ;;
    :)
      printError "Option -${OPTARG} requires an argument."
      usage
      clean 1
      ;;
    h | *)
      usage
      clean 1
      ;;
    esac
done

if [ "$ENVIRONMENT" != "cloud" ] && [ "$ENVIRONMENT" != "local" ] && [ "$ENVIRONMENT" != "dev" ]; then
    printError "Invalid environment: ${ENVIRONMENT}"
    echo ""
    usage
    clean 1
fi

echo Preparing docker image for ${ENVIRONMENT}
if [ "$BUILD" == "true" ]; then
  ./scripts/docker-prepare.sh -e $ENVIRONMENT -b # TODO: -b or -p ? 
else
  ./scripts/docker-prepare.sh -e $ENVIRONMENT
fi

source "./scripts/versions.sh"

if [ "$ENVIRONMENT" == "local" ] || [ "$ENVIRONMENT" == "dev" ] || [ "$MACHINE" == "arm64" ];
then
  # ${CONTAINER_ENGINE} buildx create --use
  ${CONTAINER_ENGINE} builder prune -f
  if [ "$ENVIRONMENT" == "local" ]; then
    if [ "$BUILD" == "true" ] || [ "$PUBLISH" == "true" ]; then
      ${CONTAINER_ENGINE} buildx create --platform ${PLATFORMS} --driver docker-container --use --bootstrap #--name starlake-builder
      ${CONTAINER_ENGINE} buildx build --platform ${PLATFORMS} --build-arg BUILD_DATE=$BUILD_DATE --build-arg VCS_REF=$VCS_REF --build-arg SL_VERSION=$SL_VERSION -t ${REGISTRY_IMAGE_LATEST} ./distrib/docker --load
      if [ "$PUBLISH" == "true" ]; then
        ${CONTAINER_ENGINE} push ${REGISTRY_IMAGE_LATEST}
      fi
    else
      ${CONTAINER_ENGINE} buildx build --build-arg BUILD_DATE=$BUILD_DATE --build-arg VCS_REF=$VCS_REF --build-arg SL_VERSION=$SL_VERSION -t ${REGISTRY_IMAGE_LATEST} --load ./distrib/docker
    fi
  else
    ${CONTAINER_ENGINE} buildx build --build-arg BUILD_DATE=$BUILD_DATE --build-arg VCS_REF=$VCS_REF --build-arg SL_VERSION=$SL_VERSION -t ${REGISTRY_IMAGE_LATEST} --load ./distrib/docker
  fi
else
  echo building for linux/$MACHINE
  export cluster=$(gcloud config get-value container/cluster 2> /dev/null)
  export zone=$(gcloud config get-value compute/zone 2> /dev/null)
  export project=$(gcloud config get-value core/project 2> /dev/null)

  ${CONTAINER_ENGINE} buildx build  --platform linux/$MACHINE --build-arg BUILD_DATE=$BUILD_DATE --build-arg VCS_REF=$VCS_REF --build-arg SL_VERSION=$SL_VERSION --output type=docker -t starlake-ai/starlake-$MACHINE:latest ./distrib/docker
  #${CONTAINER_ENGINE} push europe-west1-docker.pkg.dev/$project/starlake-docker-repo/starlake-$MACHINE:latest
fi
