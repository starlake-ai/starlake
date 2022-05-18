# shellcheck disable=SC1090

if [[ -z "$STARLAKE_ENV" ]]; then
    echo "STARLAKE_ENV not provided using default value LOCAL" 1>&2
fi

export STARLAKE_ENV="${STARLAKE_ENV:-LOCAL}"


case $STARLAKE_ENV in
    LOCAL) echo "Running  in $STARLAKE_ENV env";;
    *)              echo "$STARLAKE_ENV for STARLAKE_ENV unknown"; exit 1;;
esac

source ./env."${STARLAKE_ENV}".sh


