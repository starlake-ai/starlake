PROG_DIR=$(cd `dirname $0` && pwd)

if [ "$PWD" != "$PROG_DIR" ]; then
  echo "run command from local folder in the form ./start.sh"
  exit 1
fi

arguments="local local-bigquery hdfs"

if [[ $# -eq 0 ]] ; then
    echo "Please provide one of [$arguments]"
    exit 1
fi

if [[ $arguments =~ (^|[[:space:]])$1($|[[:space:]]) ]] ; then
 echo "running as $1 sample"
else
 echo "Please provide one of [$arguments]"
fi

HOST_DIR="$(PWD)/../$1"

HOST_IP="${HOST_IP:-$(ipconfig getifaddr en0)}"

echo "HOST_IP: $HOST_IP"
echo "HOST_DIR: $HOST_DIR"

if [[ -z "$HOST_USER" ]]; then
    echo "host username required. Please define HOST_USER" 1>&2
    exit 1
fi

if [[ -z "$HOST_PWD" ]]; then
    echo "host password required. Please define HOST_PWD" 1>&2
    exit 2
fi

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.0.2/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
echo -e "HOST_IP=$HOST_IP" >> .env
awk -v ip="$HOST_IP" '{gsub("AIRFLOW__CORE__EXECUTOR: CeleryExecutor", "AIRFLOW__CORE__EXECUTOR: CeleryExecutor\n    HOST_IP: " ip)}1' docker-compose.yaml >docker-compose.yaml.tmp && mv docker-compose.yaml.tmp docker-compose.yaml
awk  -v dir="$HOST_DIR" '{gsub("AIRFLOW__CORE__EXECUTOR: CeleryExecutor", "AIRFLOW__CORE__EXECUTOR: CeleryExecutor\n    HOST_DIR: " dir)}1' docker-compose.yaml >docker-compose.yaml.tmp && mv docker-compose.yaml.tmp docker-compose.yaml


docker compose down
docker compose up airflow-init
docker compose run airflow-worker airflow connections delete 'comet_host'
docker compose up -d
sleep 30
docker compose run airflow-worker airflow connections add 'comet_host' \
                                                      --conn-type 'ssh' \
                                                      --conn-login "$HOST_USER" \
                                                      --conn-password "$HOST_PWD" \
                                                      --conn-host "$HOST_IP" \
                                                      --conn-port '22'
