from airflow import DAG
from airflow.operators.bash_operator import BashOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 2),
    'email': ['nader.gharsallaoui.ext@louisvuitton.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG('comet_import',max_active_runs=1, catchup=False, default_args=default_args, schedule_interval='*/1 * * * *')



COMET_SPARK_CMD = "spark2-submit --keytab /etc/keytabs/importhdfs.keytab --principal importhdfs@LVU.OVH.BIGDATA --conf spark.jars.packages=\"\" --master yarn --deploy-mode client /home/airflow/program/comet-assembly-0.1.jar"
CometImport = BashOperator(
    task_id='comet_import',
    bash_command= COMET_SPARK_CMD + ' import',
    env={
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

