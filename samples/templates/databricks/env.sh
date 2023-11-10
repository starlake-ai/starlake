
if [[ -z "$SL_HOME" ]]
then
    echo "SL_HOME is not set. Please set SL_HOME to the root of your StarLake installation."
    exit 1
fi

if [[ -f "$SL_HOME/starlake.sh" ]] || [[ -f "$SL_HOME/starlake" ]]
then
    echo "SL_HOME is set to $SL_HOME"
else
    echo "SL_HOME is set to $SL_HOME, but it does not contain the StarLake CLI."
    exit 1
fi
export SL_ROOT="$(pwd)"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SL_CONNECTION=databricks

source ./.databricks-env
