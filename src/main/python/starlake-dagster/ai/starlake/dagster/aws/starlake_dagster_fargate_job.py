import os

from typing import List, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.dagster import StarlakeDagsterJob, StarlakeDagsterUtils, DagsterLogicalDatetimeConfig

from ai.starlake.aws import StarlakeFargateHelper

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy, OpExecutionContext

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_script

class StarlakeDagsterFargateJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on AWS Fargate."""

    def __init__(
            self, 
            filename: str=None, 
            module_name: str=None,
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, 
            options: dict=None,
            **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.FARGATE

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

        fargate = StarlakeFargateHelper(job=self, arguments=arguments, **kwargs)

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
                # Update the fargate arguments and environment
                fargate.arguments = arguments
                environment = fargate.environment
                environment.update(env)
                fargate.environment = environment

            command = fargate.command

            tmp_file_path = fargate.generate_script()

            tmp_path = os.path.dirname(tmp_file_path)
            context.log.info("Using temporary directory: %s" % tmp_path)

            tmp_file = os.path.basename(tmp_file_path)
            context.log.info(f"Temporary script location: {tmp_file}")

            if config.dry_run:
                output, return_code = f"Starlake command {command} execution skipped due to dry run mode.", 0
                context.log.info(output)
            else:
                output, return_code = execute_shell_script(
                        shell_script_path=tmp_file,
                        output_logging="STREAM",
                        log=context.log,
                        cwd=tmp_path,
                        env=env,
                        log_shell_command=True,
                    )

            os.unlink(tmp_file_path)

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
