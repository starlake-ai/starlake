import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

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

dag = DAG('starlake_watcher', max_active_runs=1, catchup=False, default_args=default_args, schedule_interval="*/1 * * * *")
SL_SPARK_CMD = os.environ.get('SL_SPARK_CMD', '')
SL_DOMAIN = os.environ.get('SL_DOMAIN', '')
# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='starlake_watcher',
    bash_command=SL_SPARK_CMD + ' watch ' + SL_DOMAIN,
    dag=dag)
