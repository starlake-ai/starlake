from typing import List, Optional, Union

from ai.starlake.dagster import StarlakeDagsterJob, StarlakeDagsterUtils, DagsterLogicalDatetimeConfig

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy, OpExecutionContext

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_command

class StarlakeDagsterShellJob(StarlakeDagsterJob):

    def __init__(self, filename: str=None, module_name: str=None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.SHELL

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

        sl_command = self.__class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options)

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
                assets.append(StarlakeDagsterUtils.get_asset(context, config, dataset, **kwargs))

            if transform:
                opts = arguments[-1].split(",")
                transform_opts = StarlakeDagsterUtils.get_transform_options(context, config, params, **kwargs).split(',')
                opts.extend(transform_opts)
                arguments[-1] = ",".join(opts)

            command = sl_command + f" {' '.join(arguments or [])}"

            if config.dry_run:
                output, return_code = f"Starlake command {command} execution skipped due to dry run mode.", 0
                context.log.info(output)
            else:
                context.log.info(f"Executing Starlake command: {command}")
                # Execute the shell command
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
