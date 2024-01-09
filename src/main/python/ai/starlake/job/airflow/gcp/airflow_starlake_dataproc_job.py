import os
import uuid

from typing import Union

from airflow import DAG

from ai.starlake.common import TODAY

from ai.starlake.job import StarlakePreLoadStrategy

from ai.starlake.job.airflow import AirflowStarlakeJob

from airflow.models.baseoperator import BaseOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

from airflow.utils.trigger_rule import TriggerRule

class StarlakeDataprocCluster():
    def __init__(self, options: dict, sl_env_vars:dict, sl_root:str, pool:str):
        self.project_id = AirflowStarlakeJob.get_context_var("dataproc_project_id", os.getenv("GCP_PROJECT"), options)
        self.region = AirflowStarlakeJob.get_context_var("dataproc_region", "europe-west1", options)
        self.subnet = AirflowStarlakeJob.get_context_var("dataproc_subnet", "default", options)
        self.service_account = AirflowStarlakeJob.get_context_var("dataproc_service_account", f"service-{self.project_id}@dataproc-accounts.iam.gserviceaccount.com", options)
        self.image_version = AirflowStarlakeJob.get_context_var("dataproc_image_version", "2.0.30-debian10", options)
        self.master_conf = {
            "num_instances": 1,
            "machine_type_uri": AirflowStarlakeJob.get_context_var("dataproc_master_machine_type", "n1-standard-4", options),
            "disk_config": {
                "boot_disk_type": AirflowStarlakeJob.get_context_var("dataproc_master_disk_type", "pd-standard", options), 
                "boot_disk_size_gb": int(AirflowStarlakeJob.get_context_var("dataproc_master_disk_size", "1024", options))
            }
        }
        self.worker_conf = {
            "num_instances": int(AirflowStarlakeJob.get_context_var("dataproc_num_workers", "3", options)),
            "machine_type_uri": AirflowStarlakeJob.get_context_var("dataproc_worker_machine_type", "n1-standard-4", options),
            "disk_config": {
                "boot_disk_type": AirflowStarlakeJob.get_context_var("dataproc_worker_disk_type", "pd-standard", options), 
                "boot_disk_size_gb": int(AirflowStarlakeJob.get_context_var("dataproc_worker_disk_size", "1024", options))
            }
        }
        self.cluster_properties = {
            "dataproc:dataproc.logging.stackdriver.job.driver.enable" : "true",
            "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true",
            "dataproc:dataproc.logging.stackdriver.enable": "true",
            "dataproc:jobs.file-backed-output.enable": "true",
            "spark-env:SL_HIVE": AirflowStarlakeJob.get_context_var("SL_HIVE", "false", sl_env_vars),
            "spark-env:SL_GROUPED": AirflowStarlakeJob.get_context_var("SL_GROUPED", "false", sl_env_vars),
            "spark-env:SL_ROOT": sl_root,
            "spark-env:SL_AUDIT_SINK_TYPE": AirflowStarlakeJob.get_context_var("SL_AUDIT_SINK_TYPE", "BigQuerySink", sl_env_vars),
            "spark-env:SL_SINK_REPLAY_TO_FILE": AirflowStarlakeJob.get_context_var("SL_SINK_REPLAY_TO_FILE", "false", sl_env_vars), # replay file generation causes serious performance decrease
            "spark-env:SL_MERGE_OPTIMIZE_PARTITION_WRITE": AirflowStarlakeJob.get_context_var("SL_MERGE_OPTIMIZE_PARTITION_WRITE", "true", sl_env_vars),
            "spark-env:SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE": AirflowStarlakeJob.get_context_var("SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE", "dynamic", sl_env_vars)
        }
        self.memAlloc = AirflowStarlakeJob.get_context_var("spark_executor_memory", "22g", options)
        self.numVcpu = AirflowStarlakeJob.get_context_var("spark_executor_cores", "8", options)
        self.sparkExecutorInstances = AirflowStarlakeJob.get_context_var("spark_executor_instances", "3", options)
        sparkBucket = AirflowStarlakeJob.get_context_var(var_name="spark_bucket", options=options)
        self.spark_properties = {
            "spark.hadoop.fs.defaultFS": f"gs://{sparkBucket}",
            "spark.eventLog.enabled": "true",
            "spark.executor.memory": self.memAlloc,
            "spark.executor.cores": int(self.numVcpu),
            "spark.executor.instances": str(self.sparkExecutorInstances),
            "spark.sql.sources.partitionOverwriteMode": "DYNAMIC",
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
            "spark.sql.catalogImplementation": "in-memory",
            "spark.datasource.bigquery.temporaryGcsBucket": sparkBucket,
            "spark.datasource.bigquery.allowFieldAddition": "true",
            "spark.datasource.bigquery.allowFieldRelaxation": "true",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false"
        }
        self.spark_config = {
            "memAlloc": self.memAlloc,
            "numVcpu": int(self.numVcpu),
            "sparkExecutorInstances": str(self.sparkExecutorInstances)
        }
        self.jar_list = AirflowStarlakeJob.get_context_var(var_name="spark_jar_list", options=options).split(",")
        self.main_class = AirflowStarlakeJob.get_context_var("spark_job_main_class", "ai.starlake.job.Main", options)
        self.dataproc_name = AirflowStarlakeJob.get_context_var("dataproc_name", "dataproc-cluster", options)
        self.pool = pool
        self.dag_id = str(uuid.uuid4())[:8]

    def create_dataproc_cluster(
        self,
        dag: DAG=None,
        task_id: str=None,
        project_id: str=None,
        cluster_name: str=None,
        region: str=None,
        subnet: str=None,
        service_account: str=None,
        cluster_properties: dict=None,
        image_version: str=None,
        master_conf: dict=None,
        worker_conf: dict=None,
        secondary_worker_conf: dict=None,
        is_single_node: bool=False,
        **kwargs) -> BaseOperator:
        """
        Create the Cloud Dataproc cluster.
        This operator will be flagged a success if the cluster by this name already exists.
        """
        dag_id = dag.dag_id if dag else self.dag_id
        cluster_name = f"{self.dataproc_name}-{dag_id.replace('_', '-')}" if not cluster_name else cluster_name
        task_id = f"create_{cluster_name}" if not task_id else task_id
        project_id = self.project_id if not project_id else project_id
        region = self.region if not region else region
        subnet = self.subnet if not subnet else subnet
        service_account = self.service_account if not service_account else service_account
        image_version = self.image_version if not image_version else image_version
        master_conf = self.master_conf if not master_conf else self.master_conf.copy().update(master_conf)
        worker_conf = self.worker_conf if not worker_conf else self.worker_conf.copy().update(worker_conf)
        secondary_worker_conf = {} if not secondary_worker_conf else secondary_worker_conf
        spark_events_bucket = f'dataproc-{project_id}'
        cluster_properties = self.cluster_properties if not cluster_properties else self.cluster_properties.copy().update({
            "dataproc:job.history.to-gcs.enabled": "true",
            "spark:spark.history.fs.logDirectory": f"gs://{spark_events_bucket}/tmp/spark-events/{{{{ds}}}}",
            "spark:spark.eventLog.dir": f"gs://{spark_events_bucket}/tmp/spark-events/{{{{ds}}}}",
        }).update(cluster_properties)

        cluster_config = {
            "master_config": master_conf,
            "worker_config": worker_conf,
            "config_bucket": f"dataproc-{project_id}",
            "gce_cluster_config": {
                "service_account": service_account,
                "subnetwork_uri": f"projects/{project_id}/regions/{region}/subnetworks/{subnet}",
                "internal_ip_only": True,
                "tags": ["dataproc"]
            },
            "software_config": {
                "image_version": image_version,
                "properties": {
                    **cluster_properties
                }
            },
            "lifecycle_config": {
                "idle_delete_ttl": {"seconds": 3600}
            }
        }

        if is_single_node:
            cluster_config.pop("worker_config")
            cluster_config["software_config"]["properties"]["dataproc:dataproc.allow.zero.workers"] = "true"
        elif secondary_worker_conf:
            cluster_config["secondary_worker_config"] = secondary_worker_conf

        kwargs.update({
            'pool': kwargs.get('pool', self.pool),
            'trigger_rule': kwargs.get('trigger_rule', TriggerRule.ALL_SUCCESS)
        })

        return DataprocCreateClusterOperator(
            task_id=task_id,
            project_id=project_id,
            cluster_name=cluster_name,
            cluster_config=cluster_config,
            region=region,
            **kwargs
        )

    def delete_dataproc_cluster(
        self,
        dag: DAG=None,
        task_id: str=None,
        project_id: str=None,
        cluster_name: str=None,
        region: str=None,
        **kwargs) -> BaseOperator:
        """Tears down the cluster even if there are failures in upstream tasks."""
        dag_id = dag.dag_id if dag else self.dag_id
        cluster_name = f"{self.dataproc_name}-{dag_id.replace('_', '-')}" if not cluster_name else cluster_name
        project_id = self.project_id if not project_id else project_id
        task_id = f"delete_{cluster_name}" if not task_id else task_id
        region = self.region if not region else region
        kwargs.update({
            'pool': kwargs.get('pool', self.pool),
            'trigger_rule': kwargs.get('trigger_rule', TriggerRule.NONE_SKIPPED)
        })
        return DataprocDeleteClusterOperator(
            task_id=task_id,
            project_id=project_id,
            cluster_name=cluster_name,
            region=region,
            **kwargs
        )

    def submit_starlake_job(
        self,
        task_id: str=None,
        project_id: str=None,
        cluster_name: str=None,
        region: str=None,
        arguments: list=None,
        jar_list: list=None,
        spark_properties: dict=None,
        spark_config: dict=None,
        main_class: str=None,
        retries: int=0,
        **kwargs) -> BaseOperator:
        """Create a dataproc job on the specified cluster"""
        cluster_name = f"{self.dataproc_name}-{{{{dag.dag_id.replace('_', '-')}}}}" if not cluster_name else cluster_name
        task_id = f"{cluster_name}_submit" if not task_id else task_id
        project_id = self.project_id if not project_id else project_id
        arguments = [] if not arguments else arguments
        jar_list = self.jar_list if not jar_list else jar_list
        main_class = self.main_class if not main_class else main_class
        spark_properties = self.spark_properties.copy() if not spark_properties else self.spark_properties.copy().update(spark_properties)
        spark_config = self.spark_config if not spark_config else spark_config

        if spark_config:
            spark_properties.update({
                "spark.executor.memory": spark_config.get('memAlloc', self.memAlloc),
                "spark.executor.cores": int(spark_config.get('numVcpu', self.numVcpu)),
                "spark.executor.instances": str(spark_config.get('sparkExecutorInstances', self.sparkExecutorInstances))
            })

        kwargs.update({
            'pool': kwargs.get('pool', self.pool),
            'trigger_rule': kwargs.get('trigger_rule', TriggerRule.ALL_SUCCESS)
        })

        region = self.region if not region else region
        return DataprocSubmitJobOperator(
            task_id=task_id,
            project_id=project_id,
            region=region,
            job={
                "reference": {
                    "project_id": project_id,
                    "job_id": task_id + "__" + str(uuid.uuid4())[:8]
                },
                "placement": {
                    "cluster_name": cluster_name
                },
                "spark_job": {
                    "jar_file_uris": jar_list,
                    "main_class": main_class,
                    "args": arguments,
                    "properties": {
                        **spark_properties
                    }
                }
            },
            retries=retries,
            **kwargs
        )

class AirflowStarlakeDataprocJob(AirflowStarlakeJob):
    """Airflow Starlake Dataproc Job."""
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, cluster: StarlakeDataprocCluster=None, spark_properties: dict=None, spark_config: dict=None, options: dict=None, **kwargs):
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.cluster = StarlakeDataprocCluster(options=self.options, sl_env_vars=self.sl_env_vars, sl_root=self.sl_root, pool=self.pool) if not cluster else cluster
        self.spark_properties = {} if not spark_properties else spark_properties
        self.spark_config = {} if not spark_config else spark_config

    def sl_job(self, task_id: str, arguments: list, **kwargs) -> BaseOperator:
        """Overrides AirflowStarlakeJob.sl_job()"""
        return self.cluster.submit_starlake_job(
            task_id=task_id,
            arguments=arguments,
            spark_properties=self.spark_properties,
            spark_config=self.spark_config,
            **kwargs
        )

    def pre_tasks(self, *args, **kwargs) -> Union[BaseOperator, None]:
        """Overrides AirflowStarlakeJob.pre_tasks()"""
        return self.cluster.create_dataproc_cluster(
            *args,
            **kwargs
        )

    def post_tasks(self, *args, **kwargs) -> Union[BaseOperator, None]:
        """Overrides AirflowStarlakeJob.post_tasks()"""
        return self.cluster.delete_dataproc_cluster(
            *args,
            **kwargs
        )
