import os

from typing import List, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.dagster import StarlakeDagsterJob, StarlakeDagsterUtils, DagsterLogicalDatetimeConfig

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy, OpExecutionContext

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_command

class StarlakeDagsterCloudRunJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on Google Cloud Run."""

    def __init__(
            self, 
            filename: str=None, 
            module_name: str=None,
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, 
            project_id: str=None,
            cloud_run_job_name: str=None,
            cloud_run_job_region: str=None,
            cloud_run_service_account: str = None,
            options: dict=None,
            separator:str = ' ',
            **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.project_id = __class__.get_context_var(var_name='cloud_run_project_id', default_value=os.getenv("GCP_PROJECT"), options=self.options) if not project_id else project_id
        self.cloud_run_job_name = __class__.get_context_var(var_name='cloud_run_job_name', options=self.options) if not cloud_run_job_name else cloud_run_job_name
        self.cloud_run_job_region = __class__.get_context_var('cloud_run_job_region', "europe-west1", self.options) if not cloud_run_job_region else cloud_run_job_region
        self.cloud_run_service_account = __class__.get_context_var(var_name='cloud_run_service_account', default_value="", options=self.options) if not cloud_run_service_account else cloud_run_service_account
        if self.cloud_run_service_account:
            self.impersonate_service_account = f"--impersonate-service-account {self.cloud_run_service_account}"
        else:
            self.impersonate_service_account = ""
        self.separator = separator if separator != ',' else ' '
        self.update_env_vars = self.separator.join([(f"--update-env-vars \"^{self.separator}^" if i == 0 else "") + f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())]) + "\""

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.CLOUD_RUN

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType] = None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType], optional): The optional task type. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.
        """
        env = self.sl_env(args=arguments)

        sl_command = f"{self.__class__.get_context_var('GOOGLE_CLOUD_SDK', '/usr/local/google-cloud-sdk', self.options)}/bin/gcloud beta run jobs execute {self.cloud_run_job_name} "

        separator = self.separator
        update_env_vars = self.update_env_vars
        region = self.cloud_run_job_region
        project = self.project_id
        impersonate_service_account = self.impersonate_service_account

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
        def job(context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, **kwargs):
            if dataset:
                assets.append(StarlakeDagsterUtils.get_asset(context, config, dataset))

            if transform:
                opts = arguments[-1].split(",")
                transform_opts = StarlakeDagsterUtils.get_transform_options(context, config, params).split(',')
                env.update({
                    key: value
                    for opt in transform_opts
                    if "=" in opt  # Only process valid key=value pairs
                    for key, value in [opt.split("=")]
                })
                opts.extend(transform_opts)
                arguments[-1] = ",".join(opts)

            args = f'^{separator}^' + separator.join(arguments)

            command = (
                f"{sl_command}"
                f"--args \"{args}\" "
                f"{update_env_vars} "
                f"--wait --region {region} --project {project} --format='get(metadata.name)' {impersonate_service_account}" #--task-timeout 300
            )

            if config.dry_run:
                output, return_code = f"Starlake command {command} execution skipped due to dry run mode.", 0
                context.log.info(output)
            else:
                output, return_code = execute_shell_command(
                    shell_command=command,
                    output_logging="STREAM",
                    log=context.log,
    #                cwd=self.sl_root,
                    env=env,
                    log_shell_command=True,
                )

            if return_code:
                value=f"Starlake command {command} execution failed with output: {output}"
                if retry_policy:
                    retry_count = context.retry_number
                    if retry_count < retry_policy.max_retries:
                        raise Failure(description=value)
                if failure:
                    yield Output(value=value, output_name=failure)
                elif skip_or_start:
                    context.log.info(f"Skipping Starlake command {command} execution due to skip_or_start flag.")
                    return
                else:
                    raise Failure(description=value)
            else:
                for asset in assets:
                    yield AssetMaterialization(asset_key=asset.path, description=kwargs.get("description", f"Starlake command {command} execution succeeded"))
                if dataset:
                    yield StarlakeDagsterUtils.get_materialization(context, config, dataset, **kwargs)

                yield Output(value=output, output_name=out)

        return job
