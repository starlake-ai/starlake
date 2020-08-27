Airflow DAGs
############

Comet Data Pipeline comes with native  Airflow support.
Below are DAG definitions for each of the three ingestion steps on an kerberized HDFS cluster.

Import DAG
----------

.. code:: python

    from airflow import DAG
    from airflow.operators.bash_operator import BashOperator



    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2018, 11, 2),
        'email': ['me@here.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),

    }

    dag = DAG('comet_import',max_active_runs=1, catchup=False, default_args=default_args, schedule_interval='*/1 * * * *')



    COMET_SPARK_CMD = "spark2-submit \
                            --keytab /etc/keytabs/importhdfs.keytab \
                            --principal importhdfs@MY.BIGDATA \
                            --conf spark.jars.packages=\"\" \
                            --master yarn \
                            --deploy-mode client /home/airflow/program/comet-assembly-0.1.jar"

    CometImport = BashOperator(
        task_id='comet_import',
        bash_command= COMET_SPARK_CMD + ' import',
        env={
            'COMET_DATASETS':"/project/data",
            'COMET_METADATA':"/project/metadata",
            'COMET_AREA_ACCEPTED':"working",
            'COMET_AREA_PENDING':"staging",
            'COMET_ARCHIVE':"true",
            'COMET_LAUNCHER':"airflow",
            'COMET_HIVE':"true",
            'COMET_ANALYZE':"true"
        },
        dag=dag)



Watch DAG
---------

.. code:: python

    import os
    from airflow import DAG
    from airflow.operators.bash_operator import BashOperator
    from datetime import datetime, timedelta
    from airflow.operators.slack_operator import SlackAPIPostOperator


    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2018, 11, 2),
        'email': ['me@here.com'],
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

    COMET_SPARK_CMD = "spark2-submit \
                            --keytab /etc/keytabs/importhdfs.keytab \
                            --principal importhdfs@MY.BIGDATA \
                            --conf spark.jars.packages=\"\" \
                            --master yarn \
                            --deploy-mode client /home/airflow/program/comet-assembly-0.1.jar"

    COMET_DOMAIN = os.environ.get('COMET_DOMAIN', '')
    CometWatch = BashOperator(
        task_id='comet_watcher',
        bash_command= COMET_SPARK_CMD + ' watch '+ COMET_DOMAIN,
        #on_failure_callback=slack_task(":red_circle: Task Comet Watch Failed"),
        #on_success_callback=slack_task(":ok_hand: Task Comet Watch Success"),
        env={
            'AIRFLOW_ENDPOINT':"https://airflow.my.server.com/api/experimental",
            'COMET_DATASETS':"/project/data",
            'COMET_METADATA':"/project/metadata",
            'COMET_AREA_ACCEPTED':"working",
            'COMET_AREA_PENDING':"staging",
            'COMET_ARCHIVE':"true",
            'COMET_LAUNCHER':"airflow",
            'COMET_HIVE':"true",
            'COMET_ANALYZE':"true"
        },
        dag=dag)


Ingestion DAG
-------------

.. code:: python

    from airflow import DAG
    from airflow.operators.bash_operator import BashOperator
    from datetime import datetime, timedelta
    from airflow.operators.slack_operator import SlackAPIPostOperator


    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2018, 11, 2),
        'email': ['me@here.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }

    dag = DAG('comet_ingest',max_active_runs=1 , catchup=False, default_args=default_args, schedule_interval = None)

    def slack_task(msg):
        slack_alert = SlackAPIPostOperator(
            task_id='slack_alert',
            channel="#airflow",
            token="xoxp-64071012534-475450904118-524897638692-f9a90d49fd7fb312a574b4570d557b9a",
            text = msg,
            username = 'airflow',)
        return slack_alert.execute(msg=msg)

    COMET_SPARK_CMD = "spark2-submit \
                            --keytab /etc/keytabs/importhdfs.keytab \
                            --principal importhdfs@MY.BIGDATA \
                            --conf spark.jars.packages=\"\" \
                            --conf spark.yarn.appMasterEnv.COMET_METADATA=/project/metadata \
                            --conf spark.yarn.appMasterEnv.COMET_ACCEPTED=working \
                            --conf spark.yarn.appMasterEnv.COMET_DATASETS=/project/data \
                            --master yarn \
                            --deploy-mode cluster /home/airflow/program/comet-assembly-0.1.jar"

    templated_command = COMET_SPARK_CMD + """ {{ dag_run.conf['command'] }}"""

    CometIngest = BashOperator(
        task_id='comet_ingest',
        bash_command=templated_command,
        #on_failure_callback=slack_task(":red_circle: Task Comet Ingest Failed: "),
        #on_success_callback=slack_task(":ok_hand: Task Comet Ingest Success: "),
        env={
            'COMET_DATASETS':"/project/data",
            'COMET_METADATA':"/project/metadata",
            'COMET_AREA_ACCEPTED':"working",
            'COMET_AREA_PENDING':"staging",
            'COMET_ARCHIVE':"true",
            'COMET_LAUNCHER':"airflow",
            'COMET_HIVE':"true",
            'COMET_ANALYZE':"true"
        },
        dag=dag)

