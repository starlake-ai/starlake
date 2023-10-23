if [[ -z "$SL_ENV" ]]; then
    echo "SL_ENV not provided using default value SNOWFLAKE" 1>&2
fi

export SL_ENV="${SL_ENV:-SNOWFLAKE}"

# shellcheck disable=SC1090
source ./env."${SL_ENV}".sh


cp -r ../quickstart/incoming $PWD/quickstart
