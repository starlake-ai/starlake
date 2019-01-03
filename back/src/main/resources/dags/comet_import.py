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

dag = DAG('comet_import', max_active_runs=1, catchup=False, default_args=default_args, schedule_interval="*/1 * * * *")
COMET_SPARK_CMD = os.environ.get('COMET_SPARK_CMD', '')
# To compute histogram add --conf spark.sql.statistics.histogram.enabled=true
# export COMET_SPARK_CMD="/Users/hayssams/programs/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --class com.ebiznext.comet.job.Main /Users/hayssams/git/comet/app/back/target/scala-2.11/comet-assembly-0.1.jar"
# export COMET_SPARK_CMD="/Users/hayssams/programs/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class com.ebiznext.comet.job.Main /Users/hayssams/git/comet/app/back/target/scala-2.11/comet-assembly-0.1.jar"
t1 = BashOperator(
    task_id='comet_import',
    bash_command= COMET_SPARK_CMD + ' import',
    dag=dag)

