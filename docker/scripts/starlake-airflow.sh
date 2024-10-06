#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -eo pipefail

# Check if at least one argument is passed
if [ "$#" -eq 0 ]; then
  echo "No arguments provided. Usage: starlake <command> [args...]"
  exit 1
fi

# Use all arguments passed to the script with $@
docker exec -ti starlake-api /app/starlake/starlake.sh "$@"
