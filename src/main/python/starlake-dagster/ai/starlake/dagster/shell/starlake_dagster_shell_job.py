from typing import Union

from ai.starlake.dagster import StarlakeDagsterJob

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig

from dagster import Failure, OpDefinition, Output, AssetMaterialization, AssetKey, Out

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_command

from dagster_shell.ops import ShellOpConfig

class StarlakeDagsterShellJob(StarlakeDagsterJob):

    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.

        Returns:
            OpDefinition: The Dastger node.
        """
        command = self.__class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options) + f" {' '.join(arguments)}"

        asset_key: AssetKey = kwargs.get("asset", None)

        def compute_fn(context, config: ShellOpConfig):
            output, return_code = execute_shell_command(
                shell_command=command,
                output_logging="STREAM",
                log=context.log,
                cwd=self.sl_root,
                env=self.sl_env_vars,
                log_shell_command=True,
            )

            if return_code:
                raise Failure(description=f"Starlake command {command} execution failed with output: {output}")

            if asset_key:
                yield AssetMaterialization(asset_key=asset_key.path, description=kwargs.get("description", f"Starlake command {command} execution succeeded"))

            yield Output(value=output, output_name="result")

        return OpDefinition(
            compute_fn=compute_fn,
            name=task_id,
            ins=kwargs.get("ins", {}),
            outs={"result": Out(str)},
        )
