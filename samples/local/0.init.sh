if [[ -z "$STARLAKE_ENV" ]]; then
    echo "Must provide STARLAKE_ENV in environment" 1>&2
    exit 1
fi

case $STARLAKE_ENV in
    LOCAL|HDFS|GCP) echo "Running  in $STARLAKE_ENV env";;
    *)             echo "$STARLAKE_ENV for STARLAKE_ENV unknown"; exit 1;;
esac

# shellcheck disable=SC1090
source ./env."${STARLAKE_ENV}".sh

initEnv

