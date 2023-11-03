export SL_BIN_DIR="$(pwd)/../../../distrib"
export SL_ROOT="$(pwd)"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SF_TIMEZONE=TIMESTAMP_LTZ
export SL_CONNECTION=postgres

source ./.postgres-env

if [[ -z "$POSTGRES_DATABASE" ]] || [[ -z "$DATABASE_USER" ]] || [[ -z "$DATABASE_PASSWORD" ]] || [[ -z "$POSTGRES_HOST" ]] || [[ -z "$POSTGRES_PORT" ]]; then
    echo "Must provide POSTGRES_DATABASE, DATABASE_USER, DATABASE_PASSWORD, POSTGRES_HOST, POSTGRES_PORT in environment" 1>&2
    exit 1
fi

