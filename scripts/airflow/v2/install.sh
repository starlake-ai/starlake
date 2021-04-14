AIRFLOW_VERSION=2.0.2
curl -LfO https://airflow.apache.org/docs/apache-airflow/${AIRFLOW_VERSION}/docker-compose.yaml
curl -LfO https://airflow.apache.org/docs/apache-airflow/${AIRFLOW_VERSION}/airflow.sh
chmod +x airflow.sh
mkdir -p ./dags ./logs ./plugins
#echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
docker-compose up airflow-init
echo The account created has the login airflow and the password airflow.
#docker-compose up
