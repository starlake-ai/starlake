#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -eo pipefail

# Check if at least one argument is passed
if [ "$#" -eq 0 ]; then
  echo "No arguments provided. Usage: starlake <command> [args...]"
  exit 1
fi

options=""
command="$1"
shift

arguments=()
while [ $# -gt 0 ]; do
    case "$1" in
        -o | --options)   options="$2"; shift; shift;;
        *) arguments+=("$1");shift;;
    esac
done

envs=$(echo $options | tr "," "\n")

docker_envs=()
for env in $envs; do
    docker_envs+=("-e $env")
done

docker exec ${docker_envs[*]} starlake-api /app/starlake/starlake.sh $command ${arguments[*]}
