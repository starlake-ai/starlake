import airflow
import os
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1, 0, 0),
}

dag = airflow.DAG(
    'comet_docker_watch',
    default_args=default_args,
    schedule_interval=None
)

HOST_IP = os.getenv('HOST_IP')
HOST_DIR = os.getenv('HOST_DIR')

cmd_ssh = f'cd {HOST_DIR} && ./2.watch.sh '

task_1 = SSHOperator(
    ssh_conn_id='comet_host',
    task_id='task_ssh',
    command=cmd_ssh,
    do_xcom_push=True,
    dag=dag
)

