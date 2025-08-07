import json

import uuid

from typing import List, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.dagster import StarlakeDagsterJob, StarlakeDagsterUtils, DagsterLogicalDatetimeConfig

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.common import TODAY

from ai.starlake.gcp import StarlakeDataprocClusterConfig

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy, OpExecutionContext

from dagster._core.definitions import NodeDefinition

from dagster_gcp import DataprocResource

from dagster_gcp.dataproc.resources import DataprocClient

class StarlakeDagsterDataprocJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on Google Cloud Dataproc."""

    def __init__(
            self, 
            filename: str=None, 
            module_name: str=None,
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, 
            cluster_config: StarlakeDataprocClusterConfig=None, 
            options: dict=None,
            **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.cluster_config = StarlakeDataprocClusterConfig.from_module(filename, module_name, self.options) if not cluster_config else cluster_config
        cluster_id = self.cluster_config.cluster_id
        cluster_name = f"{self.cluster_config.dataproc_name}-{cluster_id.replace('_', '-')}-{TODAY}"
        self.__dataproc__  = DataprocResource(
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region,
            cluster_name=cluster_name,
            cluster_config_dict=self.cluster_config.__config__()
        )

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.DATAPROC

    def __client__(self) -> DataprocClient:
        """Get the Dataproc client."""
        return self.__dataproc__.get_client()

    def pre_tasks(self, *args, **kwargs) -> NodeDefinition | None:
        """Overrides IStarlakeJob.pre_tasks()"""
        task_id = kwargs.get('task_id', f"create_{self.cluster_config.cluster_id.replace('-', '_')}_cluster")
        kwargs.pop('task_id', None)

        asset_key: Union[AssetKey, None] = kwargs.get("asset", None)

        @op(
            name=task_id,
            ins=kwargs.get("ins", {}),
            out={kwargs.get("out", "result"): Out(str)},
        )
        def create_dataproc_cluster(context, config: DagsterLogicalDatetimeConfig, **kwargs):
            if config.dry_run:
                output = f"Dataproc cluster {self.__dataproc__.cluster_name} creation skipped due to dry run mode."
                context.log.info(output)
            else:
                context.log.info(f"Creating Dataproc cluster {self.__dataproc__.cluster_name} with cluster details: \n{json.dumps(self.__dataproc__.cluster_config_dict, indent=2)}")
                self.__client__().create_cluster()
            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=f"Dataproc cluster {self.__dataproc__.cluster_name} created")
            yield Output(value=task_id, output_name="result")

        return create_dataproc_cluster

    def post_tasks(self, *args, **kwargs) -> NodeDefinition | None:
        """Overrides IStarlakeJob.post_tasks()"""

        task_id = kwargs.get('task_id', f"delete_{self.cluster_config.cluster_id.replace('-', '_')}_cluster")
        kwargs.pop('task_id', None)

        asset_key: Union[AssetKey, None] = kwargs.get("asset", None)

        @op(
            name=task_id,
            ins=kwargs.get("ins", {}),
            out={kwargs.get("out", "result"): Out(str)},
        )
        def delete_dataproc_cluster(context, config: DagsterLogicalDatetimeConfig, **kwargs):
            if config.dry_run:
                output = f"Dataproc cluster {self.__dataproc__.cluster_name} deletion skipped due to dry run mode."
                context.log.info(output)
            else:
                context.log.info(f"Deleting Dataproc cluster {self.__dataproc__.cluster_name}")
                self.__client__().delete_cluster()
            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=f"Dataproc cluster {self.__dataproc__.cluster_name} deleted")
            yield Output(value=task_id, output_name="result")

        return delete_dataproc_cluster

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType] = None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command within the dataproc cluster by submitting the corresponding spark job.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType], optional): The optional task type. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.
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

        if not task_type and len(arguments) > 0:
            task_type = TaskType.from_str(arguments[0])
        transform = task_type == TaskType.TRANSFORM
        params = kwargs.get('params', dict())

        assets: List[AssetKey] = kwargs.get("assets", [])

        ins=kwargs.get("ins", {})

        out:str=kwargs.get("out", "result")
        failure:str=kwargs.get("failure", None)
        skip_or_start = bool(kwargs.get("skip_or_start", False))
        outs=kwargs.get("outs", {out: Out(str, is_required=not skip_or_start and failure is None)})
        if failure:
            outs.update({failure: Out(str, is_required=False)})

        max_retries = int(kwargs.get("retries", self.retries))
        if max_retries > 0:
            retry_policy = RetryPolicy(max_retries=max_retries, delay=self.retry_delay)
        else:
            retry_policy = None

        @op(
            name=task_id,
            ins=ins,
            out=outs,
            retry_policy=retry_policy,
        )
        def submit_dataproc_job(context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, **kwargs):
            if dataset:
                assets.append(StarlakeDagsterUtils.get_asset(context, config, dataset))

            if transform:
                opts = arguments[-1].split(",")
                transform_opts = StarlakeDagsterUtils.get_transform_options(context, config, params).split(',')
                opts.extend(transform_opts)
                arguments[-1] = ",".join(opts)
                job = job_details.get("job", {})
                spark_job = job.get("spark_job", {})
                spark_job["args"] = arguments
                job["spark_job"] = spark_job
                job_details["job"] = job

            if config.dry_run:
                output = f"Starlake command {' '.join(arguments)} execution skipped due to dry run mode."
                context.log.info(output)
                result = {"status": {"state": "DONE"}}
            else:
                context.log.info(f"Submitting Spark job {job_id} to Dataproc cluster {self.__dataproc__.cluster_name} with job details: \n{json.dumps(job_details, indent=2)}")
                result = self.__client__().submit_job(job_details=job_details)

            if result.get("status", {}).get("state") != "DONE":
                value=f"Spark job {job_id} submission failed with result: {result}"
                if retry_policy:
                    retry_count = context.retry_number
                    if retry_count < retry_policy.max_retries:
                        raise Failure(description=value)
                if failure:
                    yield Output(value=value, output_name=failure)
                elif skip_or_start:
                    context.log.info(f"Skipping Starlake command {' '.join(arguments)} execution due to skip_or_start flag.")
                    return
                else:
                    raise Failure(description=value)
            else:
                for asset in assets:
                    yield AssetMaterialization(asset_key=asset.path, description=f"Spark job {job_id} submitted to Dataproc cluster {self.__dataproc__.cluster_name}")
                if dataset:
                    yield StarlakeDagsterUtils.get_materialization(context, config, dataset, **kwargs)

                yield Output(value=job_id, output_name=out)

        return submit_dataproc_job
