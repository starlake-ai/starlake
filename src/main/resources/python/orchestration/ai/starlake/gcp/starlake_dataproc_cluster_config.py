import os

import uuid

from ai.starlake.job import StarlakeOptions

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

class StarlakeDataprocMasterConfig(StarlakeDataprocMachineConfig, StarlakeOptions):
    def __init__(self, machine_type: str, disk_type: str, disk_size: int, options: dict, **kwargs):
        super().__init__(
            num_instances=1,
            machine_type=__class__.get_context_var("dataproc_master_machine_type", "n1-standard-4", options) if not machine_type else machine_type,
            disk_type=__class__.get_context_var("dataproc_master_disk_type", "pd-standard", options) if not disk_type else disk_type,
            disk_size=int(__class__.get_context_var("dataproc_master_disk_size", "1024", options)) if not disk_size else disk_size,
            **kwargs
        )

class StarlakeDataprocWorkerConfig(StarlakeDataprocMachineConfig, StarlakeOptions):
    def __init__(self, num_instances: int, machine_type: str, disk_type: str, disk_size: int, options: dict, **kwargs):
        super().__init__(
            num_instances=int(__class__.get_context_var("dataproc_num_workers", "4", options)) if num_instances is None else num_instances,
            machine_type=__class__.get_context_var("dataproc_worker_machine_type", "n1-standard-4", options) if not machine_type else machine_type,
            disk_type=__class__.get_context_var("dataproc_worker_disk_type", "pd-standard", options) if not disk_type else disk_type,
            disk_size=int(__class__.get_context_var("dataproc_worker_disk_size", "1024", options)) if not disk_size else disk_size,
            **kwargs
        )

class StarlakeDataprocClusterConfig(StarlakeOptions):
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
