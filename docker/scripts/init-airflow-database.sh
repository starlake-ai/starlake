#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE ${AIRFLOW_DB:-airflow};
    GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_DB:-airflow} TO "$POSTGRES_USER";
EOSQL