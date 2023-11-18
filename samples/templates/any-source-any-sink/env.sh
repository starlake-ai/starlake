KNOWN_ENVS="bigquery,snowflake,databricks,spark,postgres"

if [[ -z "$SL_ENV" ]]; then
    echo "Please provide the datawarehouse configuration you are using through the SL_ENV var. Valid values are $KNOWN_ENVS."
    exit 1
fi

if [[ ! $KNOWN_ENVS =~ (^|,)$SL_ENV($|,) ]]; then
    echo "Unknown env $SL_ENV: Please select one of: $KNOWN_ENVS"
    exit 1
fi

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
export SL_CONNECTION=$SL_ENV

# The file below contains the credentials for the target datawarehouse
source ./.$SL_ENV-env
