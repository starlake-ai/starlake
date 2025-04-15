if [[ -z "$SL_ENV" ]]; then
    echo "SL_ENV not provided using default value LOCAL" 1>&2
fi

export SL_ENV="${SL_ENV:-LOCAL}"


case $SL_ENV in
    LOCAL|HDFS|SNOWFLAKE) echo "Running  in $SL_ENV env";;
    *)             echo "$SL_ENV for SL_ENV unknown"; exit 1;;
esac

# shellcheck disable=SC1090
source ./env."${SL_ENV}".sh



SL_INTERNAL_SUBSTITUTE_VARS=false $SL_HOME/starlake.sh yml2xls --xls $SL_ROOT/metadata/load

