import os

from typing import List, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.dagster import StarlakeDagsterJob

from ai.starlake.aws import StarlakeFargateHelper

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment

from dagster import Failure, Output, AssetMaterialization, AssetKey, Out, op, RetryPolicy

from dagster._core.definitions import NodeDefinition

from dagster_shell import execute_shell_script

class StarlakeDagsterFargateJob(StarlakeDagsterJob):
    """A StarlakeDagsterJob that runs a starlake command on AWS Fargate."""

    def __init__(
            self, 
            filename: str, 
            module_name: str,
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

        fargate = StarlakeFargateHelper(job=self, arguments=arguments, **kwargs)

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

            command = " ".join(arguments)

            tmp_file_path = fargate.generate_script()

            tmp_path = os.path.dirname(tmp_file_path)
            context.log.info("Using temporary directory: %s" % tmp_path)

            tmp_file = os.path.basename(tmp_file_path)
            context.log.info(f"Temporary script location: {tmp_file}")

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
