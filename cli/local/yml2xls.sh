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

set -x

COMET_INTERNAL_SUBSTITUTE_VARS=false $SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS \
    --class $COMET_MAIN $COMET_JAR_FULL_NAME yml2xls --xls $COMET_ROOT/metadata/domains

