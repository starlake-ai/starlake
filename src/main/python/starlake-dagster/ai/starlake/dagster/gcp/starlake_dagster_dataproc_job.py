import json

import uuid

from typing import Union

from ai.starlake.dagster import StarlakeDagsterJob

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig

from ai.starlake.common import TODAY

from ai.starlake.gcp import StarlakeDataprocClusterConfig

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op

from dagster._core.definitions import NodeDefinition

from dagster_gcp import DataprocResource

from dagster_gcp.dataproc.resources import DataprocClient

class StarlakeDagsterDataprocJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on Google Cloud Dataproc."""

    def __init__(
            self, 
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, 
            cluster_config: StarlakeDataprocClusterConfig=None, 
            options: dict=None,
            **kwargs) -> None:
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.cluster_config = StarlakeDataprocClusterConfig(
            cluster_id="dataproc",
            dataproc_name=None,
            master_config=None, 
            worker_config=None, 
            secondary_worker_config=None, 
            idle_delete_ttl=None, 
            single_node=None, 
            options=self.options,
            **kwargs
        ) if not cluster_config else cluster_config
        cluster_id = self.cluster_config.cluster_id
        cluster_name = f"{self.cluster_config.dataproc_name}-{cluster_id.replace('_', '-')}-{TODAY}"
        self.__dataproc__  = DataprocResource(
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region,
            cluster_name=cluster_name,
            cluster_config_dict=self.cluster_config.__config__()
        )

    def __client__(self) -> DataprocClient:
        """Get the Dataproc client."""
        return self.__dataproc__.get_client()

    def pre_tasks(self, *args, **kwargs) -> NodeDefinition | None:
        """Overrides IStarlakeJob.pre_tasks()"""
        task_id = f"create_{self.cluster_config.cluster_id.replace('-', '_')}_cluster"

        asset_key: AssetKey = kwargs.get("asset", None)

        @op(
            name=task_id,
            ins=kwargs.get("ins", {}),
            out={kwargs.get("out", "result"): Out(str)},
        )
        def create_dataproc_cluster(context, **kwargs):
            context.log.info(f"Creating Dataproc cluster {self.__dataproc__.cluster_name} with cluster details: \n{json.dumps(self.__dataproc__.cluster_config_dict, indent=2)}")
            self.__client__().create_cluster()
            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=f"Dataproc cluster {self.__dataproc__.cluster_name} created")
            yield Output(value=task_id, output_name="result")

        return create_dataproc_cluster

    def post_tasks(self, *args, **kwargs) -> NodeDefinition | None:
        task_id = f"delete_{self.cluster_config.cluster_id.replace('-', '_')}_cluster"
        """Overrides IStarlakeJob.post_tasks()"""

        asset_key: AssetKey = kwargs.get("asset", None)

        @op(
            name=task_id,
            ins=kwargs.get("ins", {}),
            out={kwargs.get("out", "result"): Out(str)},
        )
        def delete_dataproc_cluster(context, **kwargs):
            context.log.info(f"Deleting Dataproc cluster {self.__dataproc__.cluster_name}")
            self.__client__().delete_cluster()
            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=f"Dataproc cluster {self.__dataproc__.cluster_name} deleted")
            yield Output(value=task_id, output_name="result")

        return delete_dataproc_cluster

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command within the dataproc cluster by submitting the corresponding spark job.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.

        Returns:
            NodeDefinition: The Dastger node.
        """
        jar_list = __class__.get_context_var(var_name="spark_jar_list", options=self.options).split(",")
        main_class = __class__.get_context_var("spark_job_main_class", "ai.starlake.job.Main", self.options)

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

        job_id = task_id + "_" + str(uuid.uuid4())[:8]

        job_details = {
            "project_id": self.__dataproc__.project_id,
            "region": self.__dataproc__.region,
            "job": {
                "reference": {
                    "project_id": self.__dataproc__.project_id,
                    "job_id": job_id
                },
                "placement": {
                    "cluster_name": self.__dataproc__.cluster_name
                },
                "spark_job": {
                    "jar_file_uris": jar_list,
                    "main_class": main_class,
                    "args": arguments,
                    "properties": {
                        **spark_config.__config__()
                    }
                }
            }
        }

        asset_key: AssetKey = kwargs.get("asset", None)

        @op(
            name=task_id,
            ins=kwargs.get("ins", {}),
            out={kwargs.get("out", "result"): Out(str)},
        )
        def submit_dataproc_job(context, **kwargs):
            context.log.info(f"Submitting Spark job {job_id} to Dataproc cluster {self.__dataproc__.cluster_name} with job details: \n{json.dumps(job_details, indent=2)}")
            result = self.__client__().submit_job(job_details=job_details)
            if result.get("status", {}).get("state") != "DONE":
                raise Failure(description=f"Spark job {job_id} submission failed with result: {result}")
            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=f"Spark job {job_id} submitted to Dataproc cluster {self.__dataproc__.cluster_name}")
            yield Output(value=job_id, output_name="result")

        return submit_dataproc_job
