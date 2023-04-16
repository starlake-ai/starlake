if [[ -z "$SL_ENV" ]]; then
    echo "SL_ENV not provided using default value LOCAL" 1>&2
fi

export SL_ENV="${SL_ENV:-LOCAL}"


case $SL_ENV in
    LOCAL) echo "Running  in $SL_ENV env";;
    *)             echo "Only available in LOCAL mode for testing"; exit 1;;
esac

../bin/spark-*/bin/spark-shell -i ./spark-shell-view-results.scala
