if [[ -z "$STARLAKE_ENV" ]]; then
    echo "STARLAKE_ENV not provided using default value LOCAL" 1>&2
fi

export STARLAKE_ENV="${STARLAKE_ENV:-LOCAL}"


case $STARLAKE_ENV in
    LOCAL) echo "Running  in $STARLAKE_ENV env";;
    *)             echo "Only available in LOCAL mode for testing"; exit 1;;
esac

/samples/bin/spark-*/bin/spark-shell -i /samples/local/spark-shell-view-results.scala
