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
    echo "Usage: $0 (-e [environment]) (-b)"
    echo ""
    echo "-e [environment] : build docker image to [environment], *cloud* by default"
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

echo Running on ${MACHINE}

while getopts "e:m:b:p" opt; do
    case ${opt} in
    e) 
        ENVIRONMENT=${OPTARG} 
        ;;
    m)
        MACHINE=${OPTARG}
        ;;
    b)
        BUILD="true"
        ;;
    p)
        PUBLISH="true"
        ;;
    \?)
        printError "Invalid option: ${OPTARG}"
        echo ""
        usage
        clean 1
        ;;
    :)
        printError "Invalid option: ${OPTARG} requires an argument"
        echo ""
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
  ./scripts/docker-prepare.sh -e $ENVIRONMENT -p
else
  ./scripts/docker-prepare.sh -e $ENVIRONMENT
fi

source "./scripts/versions.sh"

if [ "$ENVIRONMENT" == "local" ] || [ "$ENVIRONMENT" == "dev" ] || [ "$MACHINE" == "arm64" ];
then
#  docker buildx create --use
  docker builder prune -f
  if [ "$ENVIRONMENT" == "local" ]; then
    if [ "$BUILD" == "true" ] || [ "$PUBLISH" == "true" ]; then
      docker buildx create --platform ${PLATFORMS} --driver docker-container --use --bootstrap #--name starlake-builder
      docker buildx build --platform ${PLATFORMS} --build-arg BUILD_DATE=$BUILD_DATE --build-arg VCS_REF=$VCS_REF --build-arg SL_VERSION=$SL_VERSION -t ${REGISTRY_IMAGE_LATEST} ./distrib/docker --load
      if [ "$PUBLISH" == "true" ]; then
        docker push ${REGISTRY_IMAGE_LATEST}
      fi
    else
      docker buildx build --build-arg BUILD_DATE=$BUILD_DATE --build-arg VCS_REF=$VCS_REF --build-arg SL_VERSION=$SL_VERSION -t ${REGISTRY_IMAGE_LATEST} --load ./distrib/docker
    fi
  else
    docker buildx build --build-arg BUILD_DATE=$BUILD_DATE --build-arg VCS_REF=$VCS_REF --build-arg SL_VERSION=$SL_VERSION -t ${REGISTRY_IMAGE_LATEST} --load ./distrib/docker
  fi
else
  echo building for linux/$MACHINE
  export cluster=$(gcloud config get-value container/cluster 2> /dev/null)
  export zone=$(gcloud config get-value compute/zone 2> /dev/null)
  export project=$(gcloud config get-value core/project 2> /dev/null)

  docker buildx build  --platform linux/$MACHINE --build-arg BUILD_DATE=$BUILD_DATE --build-arg VCS_REF=$VCS_REF --build-arg SL_VERSION=$SL_VERSION --output type=docker -t starlake-ai/starlake-$MACHINE:latest ./distrib/docker
  #docker push europe-west1-docker.pkg.dev/$project/starlake-docker-repo/starlake-$MACHINE:latest
fi
