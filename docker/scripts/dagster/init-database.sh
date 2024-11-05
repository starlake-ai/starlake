#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE ${DAGSTER_DB:-dagster};
    GRANT ALL PRIVILEGES ON DATABASE ${DAGSTER_DB:-dagster} TO "$POSTGRES_USER";
EOSQL