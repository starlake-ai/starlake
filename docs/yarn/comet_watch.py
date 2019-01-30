import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.slack_operator import SlackAPIPostOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 2),
    'email': ['nader.gharsallaoui.ext@louisvuitton.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('comet_watcher',max_active_runs=1 , catchup=False, default_args=default_args, schedule_interval='*/1 * * * *')

def slack_task(msg):
    slack_alert = SlackAPIPostOperator(
        task_id='slack_alert',
        channel="#airflow",
        token="xoxp-64071012534-475450904118-524897638692-f9a90d49fd7fb312a574b4570d557b9a",
        text = msg,
        username = 'airflow',)
    return slack_alert.execute(msg=msg)

COMET_SPARK_CMD = "spark2-submit --keytab /etc/keytabs/importhdfs.keytab --principal importhdfs@LVU.OVH.BIGDATA --conf spark.jars.packages=\"\" --master yarn --deploy-mode client /home/airflow/program/comet-assembly-0.1.jar"
COMET_DOMAIN = os.environ.get('COMET_DOMAIN', '')
CometWatch = BashOperator(
    task_id='comet_watcher',
    bash_command= COMET_SPARK_CMD + ' watch '+ COMET_DOMAIN,
    #on_failure_callback=slack_task(":red_circle: Task Comet Watch Failed"),
    #on_success_callback=slack_task(":ok_hand: Task Comet Watch Success"),
    env={
        'AIRFLOW_ENDPOINT':"https://airflow-lvm-cloudera.as44099.com/api/experimental",
        'COMET_DATASETS':"/project/comet_ing_auto",
        'COMET_METADATA':"/project/comet_ing_auto/metadata",
        'COMET_ACCEPTED':"working",
        'COMET_PENDING':"staging",
        'COMET_STAGING':"true",
        'COMET_LAUNCHER':"airflow",
        'COMET_HIVE':"true",
        'COMET_ANALYZE':"true"
    },
    dag=dag)

