*************
Configuration
*************

Environment variables
#####################

By default, Comet expect metadata in the /tmp/metadata folder and will store ingested datasets in the /tmp/datasets folder.
Below is how the HDFS folders look like by default for the provided quickstart sample.

.. code::

    /tmp
    |-- datasets (Root folder of ingested datasets)
    |   |-- accepted (Root folder of all valid records)
    |   |   |-- hr (domain name as specified in the name attribute of the /tmp/metadata/hr.yml)
    |   |   |   `-- sellers (Schema name as specified in the /tmp/metadata/hr.yml)
    |   |   |       |-- _SUCCESS
    |   |   |       `-- part-00000-292c081b-7291-4797-b935-17bc9409b03b.snappy.parquet
    |   |   `-- sales
    |   |       |-- customers (valid records for this schema as specified in the /tmp/metadata/sales.yml)
    |   |       |   |-- _SUCCESS
    |   |       |   `-- part-00000-562501a1-34ef-4b94-b527-8e93bcbb5f89.snappy.parquet
    |   |       `-- orders (valid records for this schema as specified in the /tmp/metadata/sales.yml)
    |   |           |-- _SUCCESS
    |   |           `-- part-00000-92544093-4ae2-4a98-8df8-a5aba19a1b27.snappy.parquet
    |   |-- archive (Source files as found in the incoming folder are saved here after processing)
    |   |   |-- hr (Domain name)
    |   |   |   `-- sellers-2018-01-01.json
    |   |   `-- sales
    |   |       |-- customers-2018-01-01.psv
    |   |       `-- orders-2018-01-01.csv
    |   |-- business
    |   |   |-- hr
    |   |   `-- sales
    |   |-- ingesting (Temporary folder used during ingestion by Comet)
    |   |   |-- hr (One temporary subfolder / domain)
    |   |   `-- sales
    |   |-- pending (Source files are copied here from the incoming folder before processing)
    |   |   |-- hr (one folder / domain)
    |   |   `-- sales
    |   |-- rejected (invalid records in processed datasets are stored here)
    |   |   |-- hr (Domain name)
    |   |   |   `-- sellers (Schema name)
    |   |   |       |-- _SUCCESS
    |   |   |       `-- part-00000-aef2dde6-af24-4e20-ad88-3e5238916e57.snappy.parquet
    |   |   `-- sales
    |   |       |-- customers
    |   |       |   |-- _SUCCESS
    |   |       |   `-- part-00000-e6fa5ff9-ad29-4e5f-a5ff-549dd331fafd.snappy.parquet
    |   |       `-- orders
    |   |           |-- _SUCCESS
    |   |           `-- part-00000-6f7ba5d4-960b-4ac6-a123-87a7ab2d212f.snappy.parquet
    |   `-- unresolved (Files found in the incoming folder but do not match any schema)
    |       `-- hr
    |           `-- dummy.json
    `-- metadata (Root of metadata files)
        |-- domains (all domain definition files are located in this folder)
        |   |-- hr.yml (One definition file / domain)
        |   `-- sales.yml
        `-- types (All semantic types are defined here)
            |-- default.yml (Default semantic types)
            `-- types.yml (User defined semantic types, overwrite default ones)



By setting the env vars below, you may change default settings.

.. csv-table::
   :widths: 25 50 25

   Env. Var, Description, Default value
   COMET_DATASETS,Once imported where the datasets are stored in HDFS,hdfs:///tmp/datasets
   COMET_METADATA,HDFS root fodler where domains and types metadata are stored,/tmp/metadata
   COMET_ARCHIVE,Should we archive on HDFS the incoming files once they are ingested,true
   COMET_LAUNCHER,Valid values are airflow or simple,simple
   COMET_HIVE,Should be create external tables for ingested files?,true
   COMET_ANALYZE,Should we computed basic statistics (required COMET_HIVE to be set to true) ?,true
   COMET_WRITE_FORMAT,How ingested files are stored (parquet / orc / json / csv / avro),parquet
   COMET_AREA_PENDING,In $COMET_DATASET folder how the pending folder should be named,pending
   COMET_AREA_UNRESOLVED,In $COMET_DATASET folder how the unresolved folder should be named,unresolved
   COMET_AREA_ARCHIVE,In $COMET_DATASET folder how the archive folder should be named,archive
   COMET_AREA_INGESTING,In $COMET_DATASET folder how the ingesting folder should be named,ingesting
   COMET_AREA_ACCEPTED,In $COMET_DATASET folder how the accepted folder should be named,accepted
   COMET_AREA_REJECTED,In $COMET_DATASET folder how the rejected folder should be named,rejected
   COMET_AREA_BUSINESS,In $COMET_DATASET folder how the business folder should be named,business
   AIRFLOW_ENDPOINT,Airflow endpoint. Used when COMET_LAUNCHER is set to airflow,http://127.0.0.1:8080/api/experimental

.. note::
  When running on Cloudera 5.X.X prefer ORC to Parquet for the COMET_WRITE_FORMAT since Cloudera comes with Hive 1.1 which does
  not support date/timestamp fields or else simply treat dates / timestamps as strings. See HIVE_6394_


.. note::
  When running Spark on YARN in cluster mode, environment variables need to be set using the spark.yarn.appMasterEnv.[EnvironmentVariableName]

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


.. _HIVE_6394: https://issues.apache.org/jira/browse/HIVE-6394


