if [[ -z "$SL_ENV" ]]; then
    echo "SL_ENV not provided using default value LOCAL" 1>&2
fi

export SL_ENV="${SL_ENV:-SNOWFLAKE}"

case $SL_ENV in
    LOCAL)
      echo "Running  in $SL_ENV env"
      rm quickstart/metadata/application.conf 2>/dev/null
      ;;
    SNOWFLAKE)
       echo "Running  in $SL_ENV env"
       cp application.snowflake.conf quickstart/metadata/application.conf
       ;;
    *)             echo "$SL_ENV for SL_ENV unknown"; exit 1;;
esac

# shellcheck disable=SC1090
source ./env."${SL_ENV}".sh


cp -r ../quickstart/incoming $PWD/quickstart
