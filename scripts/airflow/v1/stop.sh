docker ps -f "status=running" -f "label=airflow=v1" --format '{{.ID}}' | xargs docker stop
