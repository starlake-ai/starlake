import os

from typing import Union

from ai.starlake.dagster import StarlakeDagsterJob

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_command

class StarlakeDagsterCloudRunJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on Google Cloud Run."""

    def __init__(
            self, 
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, 
            project_id: str=None,
            cloud_run_job_name: str=None,
            cloud_run_job_region: str=None,
            options: dict=None,
            separator:str = ' ',
            **kwargs) -> None:
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.project_id = __class__.get_context_var(var_name='cloud_run_project_id', default_value=os.getenv("GCP_PROJECT"), options=self.options) if not project_id else project_id
        self.cloud_run_job_name = __class__.get_context_var(var_name='cloud_run_job_name', options=self.options) if not cloud_run_job_name else cloud_run_job_name
        self.cloud_run_job_region = __class__.get_context_var('cloud_run_job_region', "europe-west1", self.options) if not cloud_run_job_region else cloud_run_job_region
        self.separator = separator if separator != ',' else ' '
        self.update_env_vars = self.separator.join([(f"--update-env-vars \"^{self.separator}^" if i == 0 else "") + f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())]) + "\""

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.

        Returns:
            NodeDefinition: The Dastger node.
        """
        args = f'^{self.separator}^' + self.separator.join(arguments)
        command = (
            f"{self.__class__.get_context_var('GOOGLE_CLOUD_SDK', '/usr/local/google-cloud-sdk', self.options)}/bin/gcloud beta run jobs execute {self.cloud_run_job_name} "
            f"--args \"{args}\" "
            f"{self.update_env_vars} "
            f"--wait --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)'" #--task-timeout 300 
        )

        asset_key: AssetKey = kwargs.get("asset", None)

        @op(
            name=task_id,
            ins=kwargs.get("ins", {}),
            out={kwargs.get("out", "result"): Out(str)},
        )
        def job(context, **kwargs):
            output, return_code = execute_shell_command(
                shell_command=command,
                output_logging="STREAM",
                log=context.log,
#                cwd=self.sl_root,
                env=self.sl_env_vars,
                log_shell_command=True,
            )

            if return_code:
                raise Failure(description=f"Starlake command {command} execution failed with output: {output}")

            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=kwargs.get("description", f"Starlake command {command} execution succeeded"))

            yield Output(value=output, output_name="result")

        return job