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

dag = DAG('comet_watcher', catchup=False, default_args=default_args, schedule_interval="*/5 * * * *")

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='comet_watcher',
    bash_command='/Users/hayssams/programs/spark-2.3.2-bin-hadoop2.7/bin/spark-submit --class com.ebiznext.comet.job.Main /Users/hayssams/git/comet/back/target/scala-2.11/comet-assembly-0.1.jar watch',
    dag=dag)

