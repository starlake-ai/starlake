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

if [[ "$0" != "./scripts/docker-prepare.sh" ]]; then
    echo "ERROR: start docker-prepare.sh from the starlake root directory as follows: ./scripts/docker-prepare.sh"
    clean 1
fi

ENVIRONMENT="cloud"
PUBLISH="false"
BUILD="false"

while getopts "e:p:b" opt; do
    case ${opt} in
    e) 
        ENVIRONMENT=${OPTARG} 
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

echo running for ${ENVIRONMENT}

source "./scripts/versions.sh"

if [ "$PUBLISH" == "true" ]; then
    echo "Publishing the application"
    sbt ++$SCALA_VERSION clean publish
    if [ $? -ne 0 ]; then
        printError "Failed to publish the application"
        clean 1
    fi
elif [ "$BUILD" == "true" ]; then
    echo "Building the application"
    SBT_OPTS="-Xss4M -Xms1g -Xmx4g" sbt ++$SCALA_VERSION clean assembly
    if [ $? -ne 0 ]; then
        printError "Failed to build the application"
        clean 1
    fi
else
    echo "Skipping the publication"
fi

if [ "${ENVIRONMENT}" != "dev" ]; then
  if [ "$ENVIRONMENT" == "local" ]; then
    rm -f ./starlake/bin/sl/*.jar
    export ENABLE_ALL=false
  fi
  if [ ! -d "starlake" ]; then
      mkdir -p starlake
  fi
  if [ ! -f starlake.sh ]; then cp distrib/starlake.sh starlake/starlake.sh; fi && chmod +x starlake/starlake.sh

  cd starlake

  echo installing starlake ${SL_VERSION}
  ./starlake.sh install

  cd ..
fi

rm -rf distrib/docker
mkdir -p distrib/docker
cp Dockerfile distrib/docker

mkdir -p distrib/docker/starlake
cp -r starlake/* distrib/docker/starlake

if [ "$PUBLISH" == "false" ] &&  [ "$BUILD" == "true" ]; then
    echo "Updating starlake core jar"
    rm -f distrib/docker/starlake/bin/sl/*.jar
    cp target/scala-${SCALA_VERSION}/starlake-core_${SCALA_VERSION}-${SL_VERSION}-assembly.jar distrib/docker/starlake/bin/sl/
else
    echo "Skipping updating starlake core jar"
fi