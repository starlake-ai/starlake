import os

from typing import List, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.dagster import StarlakeDagsterJob

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_command

class StarlakeDagsterCloudRunJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on Google Cloud Run."""

    def __init__(
            self, 
            filename: str, 
            module_name: str,
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

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.
        """
        env = self.sl_env(args=arguments)

        args = f'^{self.separator}^' + self.separator.join(arguments)
        command = (
            f"{self.__class__.get_context_var('GOOGLE_CLOUD_SDK', '/usr/local/google-cloud-sdk', self.options)}/bin/gcloud beta run jobs execute {self.cloud_run_job_name} "
            f"--args \"{args}\" "
            f"{self.update_env_vars} "
            f"--wait --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)' {self.impersonate_service_account}" #--task-timeout 300
        )

        assets: List[AssetKey] = kwargs.get("assets", [])
        if dataset:
            assets.append(self.to_event(dataset))

        ins=kwargs.get("ins", {})

        out:str=kwargs.get("out", "result")
        failure:str=kwargs.get("failure", None)
        outs=kwargs.get("outs", {out: Out(str, is_required=failure is None)})
        if failure:
            outs.update({failure: Out(str, is_required=False)})

        if self.retries:
            retry_policy = RetryPolicy(max_retries=self.retries, delay=self.retry_delay)
        else:
            retry_policy = None

        @op(
            name=task_id,
            ins=ins,
            out=outs,
            retry_policy=retry_policy,
        )
        def job(context, **kwargs):
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
                if failure:
                    if retry_policy:
                        retry_count = context.retry_number
                        if retry_count < retry_policy.max_retries:
                            raise Failure(description=value)
                        else:
                            yield Output(value=value, output_name=failure)
                    else:
                        yield Output(value=value, output_name=failure)
                else:
                    raise Failure(description=value)
            else:
                for asset in assets:
                    yield AssetMaterialization(asset_key=asset.path, description=kwargs.get("description", f"Starlake command {command} execution succeeded"))

                yield Output(value=output, output_name=out)

        return job
