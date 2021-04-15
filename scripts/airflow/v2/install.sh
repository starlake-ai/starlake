python3 -m venv comet-airflow-venv
source comet-airflow-venv/bin/activate
AIRFLOW_VERSION=2.0.1
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.6
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip3 install "apache-airflow[postgres,google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
export AIRFLOW_HOME=`pwd`
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:///$AIRFLOW_HOME/airflow.db
export AIRFLOW__CORE__SQL_ALCHEMY_CONN='postgresql+psycopg2://airflow_user:airflow_user@localhost:5432/airflow_db'
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__WEBSERVER__SECRET_KEY=c3c06ab35a03f558ca855f8f70c11f8d8ec275330ade17eff14f22dd91d5
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=9090
export AIRFLOW__WEBSERVER__BASE_URL=http://localhost:$AIRFLOW__WEBSERVER__WEB_SERVER_PORT
export AIRFLOW__CLI__ENDPOINT_URL=http://localhost:$AIRFLOW__WEBSERVER__WEB_SERVER_PORT

airflow db init
airflow users create --username airflow --firstname airflow --lastname airflow --role Admin --email airflow@starlake.ai --password airflow

#CREATE DATABASE airflow_db;
#CREATE USER airflow_user WITH PASSWORD 'airflow_user';
#GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
