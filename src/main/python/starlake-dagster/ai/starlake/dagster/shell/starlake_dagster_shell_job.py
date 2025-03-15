from typing import List, Optional, Union

from ai.starlake.dagster import StarlakeDagsterJob

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_command

class StarlakeDagsterShellJob(StarlakeDagsterJob):

    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.SHELL

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            OpDefinition: The Dagster node.
        """
        found = False

        import os
        env = os.environ.copy() # Copy the current environment variables
        env.update(self.sl_env_vars.copy()) # Copy the current sl env variables

        for index, arg in enumerate(arguments):
            if arg == "--options" and arguments.__len__() > index + 1:
                opts = arguments[index+1]
                if opts.strip().__len__() > 0:
                    temp = self.sl_env_vars.copy() # Copy the current sl env variables
                    temp.update({
                        key: value
                        for opt in opts.split(",")
                        if "=" in opt  # Only process valid key=value pairs
                        for key, value in [opt.split("=")]
                    })
                    options = ",".join([f"{key}={value}" for i, (key, value) in enumerate(temp.items())])
                    for opt in opts.split(","):
                        if "=" not in opt:
                            options += f",{opt}"
                else:
                    options = ",".join([f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())])
                arguments[index+1] = options
                found = True
                break

        if not found:
            arguments.append("--options")
            arguments.append(",".join([f"{key}={value}" for key, value in self.sl_env_vars.items()]))

        command = self.__class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options) + f" {' '.join(arguments or [])}"

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
                cwd=self.sl_root,
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
