import os
from datetime import datetime, timedelta

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('comet_bqload', max_active_runs=1, catchup=False, default_args=default_args, schedule_interval=None)

COMET_SPARK_CMD = os.environ.get('COMET_SPARK_CMD', '')

templated_command = COMET_SPARK_CMD + """ {{ dag_run.conf['command'] }}"""

t1 = BashOperator(
    task_id='comet_bqload',
    bash_command=templated_command,
    dag=dag)
