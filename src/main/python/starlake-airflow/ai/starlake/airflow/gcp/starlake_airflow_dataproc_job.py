import os
import uuid

from typing import Union

from ai.starlake.common import TODAY

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeAirflowOptions

from airflow.models.baseoperator import BaseOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

from airflow.utils.trigger_rule import TriggerRule

class StarlakeDataprocMachineConfig():
    def __init__(self, num_instances: int, machine_type: str, disk_type: str, disk_size: int, **kwargs):
        super().__init__()
        self.num_instances = num_instances
        self.machine_type = machine_type
        self.disk_type = disk_type
        self.disk_size = disk_size
        self.kwargs = kwargs

    def __config__(self):
        return dict({
            "num_instances": self.num_instances,
            "machine_type_uri": self.machine_type,
            "disk_config": {
                "boot_disk_type": self.disk_type, 
                "boot_disk_size_gb": self.disk_size
            }
        }, **self.kwargs)

class StarlakeDataprocMasterConfig(StarlakeDataprocMachineConfig, StarlakeAirflowOptions):
    def __init__(self, machine_type: str, disk_type: str, disk_size: int, options: dict, **kwargs):
        super().__init__(
            num_instances=1,
            machine_type=__class__.get_context_var("dataproc_master_machine_type", "n1-standard-4", options) if not machine_type else machine_type,
            disk_type=__class__.get_context_var("dataproc_master_disk_type", "pd-standard", options) if not disk_type else disk_type,
            disk_size=int(__class__.get_context_var("dataproc_master_disk_size", "1024", options)) if not disk_size else disk_size,
            **kwargs
        )

class StarlakeDataprocWorkerConfig(StarlakeDataprocMachineConfig, StarlakeAirflowOptions):
    def __init__(self, num_instances: int, machine_type: str, disk_type: str, disk_size: int, options: dict, **kwargs):
        super().__init__(
            num_instances=int(__class__.get_context_var("dataproc_num_workers", "4", options)) if num_instances is None else num_instances,
            machine_type=__class__.get_context_var("dataproc_worker_machine_type", "n1-standard-4", options) if not machine_type else machine_type,
            disk_type=__class__.get_context_var("dataproc_worker_disk_type", "pd-standard", options) if not disk_type else disk_type,
            disk_size=int(__class__.get_context_var("dataproc_worker_disk_size", "1024", options)) if not disk_size else disk_size,
            **kwargs
        )

class StarlakeDataprocClusterConfig(StarlakeAirflowOptions):
    def __init__(self, cluster_id:str, dataproc_name:str, master_config: StarlakeDataprocMasterConfig, worker_config: StarlakeDataprocWorkerConfig, secondary_worker_config: StarlakeDataprocWorkerConfig, idle_delete_ttl: int, single_node: bool, options: dict, **kwargs):
        super().__init__()
        options = {} if not options else options
        sl_env_vars = __class__.get_sl_env_vars(options)
        sl_root = __class__.get_sl_root(options)
        self.project_id = __class__.get_context_var("dataproc_project_id", os.getenv("GCP_PROJECT"), options)
        self.region = __class__.get_context_var("dataproc_region", "europe-west1", options)
        self.subnet = __class__.get_context_var("dataproc_subnet", "default", options)
        self.service_account = __class__.get_context_var("dataproc_service_account", f"service-{self.project_id}@dataproc-accounts.iam.gserviceaccount.com", options)
        self.image_version = __class__.get_context_var("dataproc_image_version", "2.2-debian12", options)
        self.master_config = StarlakeDataprocMasterConfig(machine_type=None, disk_type=None, disk_size=None, options=options) if not master_config else master_config
        self.worker_config = StarlakeDataprocWorkerConfig(num_instances=None, machine_type=None, disk_type=None, disk_size=None, options=options) if not worker_config else worker_config
        self.secondary_worker_config = secondary_worker_config
        self.idle_delete_ttl = int(__class__.get_context_var("dataproc_idle_delete_ttl", "3600", options)) if not idle_delete_ttl else idle_delete_ttl
        self.single_node = self.worker_config.num_instances <= 0 if not single_node else single_node
        self.cluster_properties = dict({
            "dataproc:dataproc.logging.stackdriver.job.driver.enable" : "true",
            "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true",
            "dataproc:dataproc.logging.stackdriver.enable": "true",
            "dataproc:jobs.file-backed-output.enable": "true",
            "spark-env:SL_HIVE": __class__.get_context_var("SL_HIVE", "false", sl_env_vars),
            "spark-env:SL_GROUPED": __class__.get_context_var("SL_GROUPED", "true", sl_env_vars),
            "spark-env:SL_ROOT": sl_root,
            "spark-env:SL_AUDIT_SINK_TYPE": __class__.get_context_var("SL_AUDIT_SINK_TYPE", "BigQuerySink", sl_env_vars),
            "spark-env:SL_SINK_REPLAY_TO_FILE": __class__.get_context_var("SL_SINK_REPLAY_TO_FILE", "false", sl_env_vars), # replay file generation causes serious performance decrease
            "spark-env:SL_MERGE_OPTIMIZE_PARTITION_WRITE": __class__.get_context_var("SL_MERGE_OPTIMIZE_PARTITION_WRITE", "true", sl_env_vars),
            "spark-env:SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE": __class__.get_context_var("SL_SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE", "dynamic", sl_env_vars)
        }, **kwargs)
        self.cluster_id = str(uuid.uuid4())[:8] if not cluster_id else cluster_id
        self.dataproc_name = __class__.get_context_var("dataproc_name", "dataproc-cluster", options) if not dataproc_name else dataproc_name

    def __config__(self, **kwargs):
        cluster_properties = dict(self.cluster_properties, **kwargs)
        cluster_config = {
            "master_config": self.master_config.__config__(),
            "worker_config": self.worker_config.__config__(),
            "config_bucket": f"dataproc-{self.project_id}",
            "gce_cluster_config": {
                "service_account": self.service_account,
                "subnetwork_uri": f"projects/{self.project_id}/regions/{self.region}/subnetworks/{self.subnet}",
                "internal_ip_only": True,
                "tags": ["dataproc"]
            },
            "software_config": {
                "image_version": self.image_version,
                "properties": {
                    **cluster_properties
                }
            },
            "lifecycle_config": {
                "idle_delete_ttl": {"seconds": self.idle_delete_ttl}
            }
        }
        if self.single_node:
            cluster_config.pop("worker_config")
            cluster_config["software_config"]["properties"]["dataproc:dataproc.allow.zero.workers"] = "true"
        elif self.secondary_worker_config:
            cluster_config["secondary_worker_config"] = self.secondary_worker_config.__config__()
        return cluster_config

class StarlakeDataprocCluster(StarlakeAirflowOptions):
    def __init__(self, cluster_config: StarlakeDataprocClusterConfig, options: dict, pool:str, **kwargs):
        super().__init__()

        self.options = {} if not options else options

        self.cluster_config = StarlakeDataprocClusterConfig(
            cluster_id=None,
            dataproc_name=None,
            master_config=None, 
            worker_config=None, 
            secondary_worker_config=None, 
            idle_delete_ttl=None, 
            single_node=None, 
            options=self.options,
            **kwargs
        ) if not cluster_config else cluster_config

        self.pool = pool

    def create_dataproc_cluster(
        self,
        cluster_id: str=None,
        task_id: str=None,
        cluster_name: str=None,
        **kwargs) -> BaseOperator:
        """
        Create the Cloud Dataproc cluster.
        This operator will be flagged a success if the cluster by this name already exists.
        """
        cluster_id = self.cluster_config.cluster_id if not cluster_id else cluster_id
        cluster_name = f"{self.cluster_config.dataproc_name}-{cluster_id.replace('_', '-')}-{TODAY}" if not cluster_name else cluster_name
        task_id = f"create_{cluster_id.replace('-', '_')}_cluster" if not task_id else task_id

        kwargs.update({
            'pool': kwargs.get('pool', self.pool),
            'trigger_rule': kwargs.get('trigger_rule', TriggerRule.ALL_SUCCESS)
        })

        spark_events_bucket = f'dataproc-{self.cluster_config.project_id}'

        return DataprocCreateClusterOperator(
            task_id=task_id,
            project_id=self.cluster_config.project_id,
            cluster_name=cluster_name,
            cluster_config=self.cluster_config.__config__(**{
                    "dataproc:job.history.to-gcs.enabled": "true",
                    "spark:spark.history.fs.logDirectory": f"gs://{spark_events_bucket}/tmp/spark-events/{{{{ds}}}}",
                    "spark:spark.eventLog.dir": f"gs://{spark_events_bucket}/tmp/spark-events/{{{{ds}}}}",
            }),
            region=self.cluster_config.region,
            **kwargs
        )

    def delete_dataproc_cluster(
        self,
        cluster_id: str=None,
        task_id: str=None,
        cluster_name: str=None,
        **kwargs) -> BaseOperator:
        """Tears down the cluster even if there are failures in upstream tasks."""
        cluster_id = self.cluster_config.cluster_id if not cluster_id else cluster_id
        cluster_name = f"{self.cluster_config.dataproc_name}-{cluster_id.replace('_', '-')}-{TODAY}" if not cluster_name else cluster_name
        task_id = f"delete_{cluster_id.replace('-', '_')}_cluster" if not task_id else task_id
        kwargs.update({
            'pool': kwargs.get('pool', self.pool),
            'trigger_rule': kwargs.get('trigger_rule', TriggerRule.NONE_SKIPPED)
        })
        return DataprocDeleteClusterOperator(
            task_id=task_id,
            project_id=self.cluster_config.project_id,
            cluster_name=cluster_name,
            region=self.cluster_config.region,
            **kwargs
        )

    def submit_starlake_job(
        self,
        cluster_id: str=None,
        task_id: str=None,
        cluster_name: str=None,
        spark_config: StarlakeSparkConfig=None,
        jar_list: list=None,
        main_class: str=None,
        arguments: list=None,
        **kwargs) -> BaseOperator:
        """Create a dataproc job on the specified cluster"""
        cluster_id = self.cluster_config.cluster_id if not cluster_id else cluster_id
        cluster_name = f"{self.cluster_config.dataproc_name}-{cluster_id.replace('_', '-')}-{TODAY}" if not cluster_name else cluster_name
        task_id = f"{cluster_id}_submit" if not task_id else task_id
        arguments = [] if not arguments else arguments
        jar_list = __class__.get_context_var(var_name="spark_jar_list", options=self.options).split(",") if not jar_list else jar_list
        main_class = __class__.get_context_var("spark_job_main_class", "ai.starlake.job.Main", self.options) if not main_class else main_class

        sparkBucket = __class__.get_context_var(var_name="spark_bucket", options=self.options)
        spark_properties = {
            "spark.hadoop.fs.defaultFS": f"gs://{sparkBucket}",
            "spark.eventLog.enabled": "true",
            "spark.sql.sources.partitionOverwriteMode": "DYNAMIC",
            "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
            "spark.sql.catalogImplementation": "in-memory",
            "spark.datasource.bigquery.temporaryGcsBucket": sparkBucket,
            "spark.datasource.bigquery.allowFieldAddition": "true",
            "spark.datasource.bigquery.allowFieldRelaxation": "true",
            "spark.dynamicAllocation.enabled": "false",
            "spark.shuffle.service.enabled": "false"
        }
        spark_config = StarlakeSparkConfig(memory=None, cores=None, instances=None, cls_options=self, options=self.options, **spark_properties) if not spark_config else StarlakeSparkConfig(memory=spark_config.memory, cores=spark_config.cores, instances=spark_config.instances, cls_options=self, options=self.options, **dict(spark_properties, **spark_config.spark_properties))

        kwargs.update({
            'pool': kwargs.get('pool', self.pool),
            'trigger_rule': kwargs.get('trigger_rule', TriggerRule.ALL_SUCCESS)
        })

        job_id = task_id + "_" + str(uuid.uuid4())[:8]

        return DataprocSubmitJobOperator(
            task_id=task_id,
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region,
            job={
                "reference": {
                    "project_id": self.cluster_config.project_id,
                    "job_id": job_id
                },
                "placement": {
                    "cluster_name": cluster_name
                },
                "spark_job": {
                    "jar_file_uris": jar_list,
                    "main_class": main_class,
                    "args": arguments,
                    "properties": {
                        **spark_config.__config__()
                    }
                }
            },
            **kwargs
        )

class StarlakeAirflowDataprocJob(StarlakeAirflowJob):
    """Airflow Starlake Dataproc Job."""
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, cluster: StarlakeDataprocCluster=None, options: dict=None, **kwargs):
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.cluster = StarlakeDataprocCluster(cluster_config=None, options=self.options, pool=self.pool) if not cluster else cluster

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
        """Overrides StarlakeAirflowJob.sl_job()
        Generate the Airflow task that will run the starlake command.
        
        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
        
        Returns:
            BaseOperator: The Airflow task.
        """
        return self.cluster.submit_starlake_job(
            task_id=task_id,
            arguments=arguments,
            spark_config=spark_config,
            **kwargs
        )

    def pre_tasks(self, *args, **kwargs) -> Union[BaseOperator, None]:
        """Overrides StarlakeAirflowJob.pre_tasks()"""
        return self.cluster.create_dataproc_cluster(
            *args,
            **kwargs
        )

    def post_tasks(self, *args, **kwargs) -> Union[BaseOperator, None]:
        """Overrides StarlakeAirflowJob.post_tasks()"""
        return self.cluster.delete_dataproc_cluster(
            *args,
            **kwargs
        )
