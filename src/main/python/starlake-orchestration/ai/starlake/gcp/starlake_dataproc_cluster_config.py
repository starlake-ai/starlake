import os

import sys

import uuid

from ai.starlake.common import MissingEnvironmentVariable
from ai.starlake.job import StarlakeOptions

from typing import Optional

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

    @classmethod
    def from_module(cls, filename: str, module_name: str, options: dict):
        caller_globals = sys.modules[module_name].__dict__
        cluster_config_name = cls.get_context_var("cluster_config_name", filename.replace(".py", "").replace(".pyc", "").lower(), options)
        def default_dataproc_master_config(*args, **kwargs) -> StarlakeDataprocMasterConfig:
            return cls(
                machine_type=caller_globals.get('dataproc_master_machine_type', None), 
                disk_type=caller_globals.get('dataproc_master_disk_type', None),
                disk_size=caller_globals.get('dataproc_master_disk_size', None), 
                options=options,
                **kwargs
            )
        dataproc_master_config = getattr(module_name, "get_dataproc_master_config", default_dataproc_master_config)
        return dataproc_master_config(cluster_config_name, **caller_globals.get('dataproc_master_properties', {}))

class StarlakeDataprocWorkerConfig(StarlakeDataprocMachineConfig, StarlakeOptions):
    def __init__(self, num_instances: int, machine_type: str, disk_type: str, disk_size: int, options: dict, **kwargs):
        super().__init__(
            num_instances=int(__class__.get_context_var("dataproc_num_workers", "4", options)) if num_instances is None else num_instances,
            machine_type=__class__.get_context_var("dataproc_worker_machine_type", "n1-standard-4", options) if not machine_type else machine_type,
            disk_type=__class__.get_context_var("dataproc_worker_disk_type", "pd-standard", options) if not disk_type else disk_type,
            disk_size=int(__class__.get_context_var("dataproc_worker_disk_size", "1024", options)) if not disk_size else disk_size,
            **kwargs
        )

    @classmethod
    def from_module(cls, filename: str, module_name: str, options: dict):
        caller_globals = sys.modules[module_name].__dict__
        cluster_config_name = cls.get_context_var("cluster_config_name", filename.replace(".py", "").replace(".pyc", "").lower(), options)
        def default_dataproc_worker_config(*args, **kwargs) -> StarlakeDataprocWorkerConfig:
            return cls(
                num_instances=caller_globals.get('dataproc_worker_num_instances', None),
                machine_type=caller_globals.get('dataproc_worker_machine_type', None), 
                disk_type=caller_globals.get('dataproc_worker_disk_type', None),
                disk_size=caller_globals.get('dataproc_worker_disk_size', None), 
                options=options,
                **kwargs
            )
        dataproc_worker_config = getattr(module_name, "get_dataproc_worker_config", default_dataproc_worker_config)
        return dataproc_worker_config(cluster_config_name, **caller_globals.get('dataproc_worker_properties', {}))

class StarlakeDataprocClusterConfig(StarlakeOptions):
    def __init__(self, cluster_id:Optional[str] = None, dataproc_name:Optional[str] = None, master_config: Optional[StarlakeDataprocMasterConfig] = None, worker_config: Optional[StarlakeDataprocWorkerConfig] = None, secondary_worker_config: Optional[StarlakeDataprocWorkerConfig] = None, idle_delete_ttl: Optional[int] = None, single_node: Optional[bool] = None, options: Optional[dict] = None, **kwargs):
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
        try:
            import json
            self.metadata = json.loads(__class__.get_context_var("dataproc_cluster_metadata", options))
        except MissingEnvironmentVariable:
            self.metadata = {}

    @classmethod
    def from_module(cls, filename: str, module_name: str, options: dict):
        caller_globals = sys.modules[module_name].__dict__
        cluster_config_name = cls.get_context_var("cluster_config_name", filename.replace(".py", "").replace(".pyc", "").lower(), options)
        cluster_id = caller_globals.get("cluster_id", cluster_config_name)
        dataproc_name = caller_globals.get("dataproc_name", None)
        master_config = StarlakeDataprocMasterConfig.from_module(filename, module_name, options)
        worker_config = StarlakeDataprocWorkerConfig.from_module(filename, module_name, options)
        dataproc_secondary_worker_config = getattr(module_name, "get_dataproc_secondary_worker_config", lambda dag_name: None)
        secondary_worker_config = dataproc_secondary_worker_config(cluster_config_name)
        idle_delete_ttl=caller_globals.get('dataproc_idle_delete_ttl', None)
        single_node=caller_globals.get('dataproc_single_node', None)
        return cls(
            cluster_id=cluster_id,
            dataproc_name=dataproc_name,
            master_config=master_config,
            worker_config=worker_config,
            secondary_worker_config=secondary_worker_config,
            idle_delete_ttl=idle_delete_ttl,
            single_node=single_node,
            options=options,
            **caller_globals.get('dataproc_cluster_properties', {})
        )

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
                "tags": ["dataproc"],
                "metadata": {
                    **self.metadata
                }
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

    def __str__(self):
        return f"StarlakeDataprocClusterConfig({self.__config__()})"