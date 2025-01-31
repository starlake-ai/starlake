from typing import Any, Dict, Optional, Union

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment

from ai.starlake.airflow import StarlakeAirflowJob

from ai.starlake.aws import StarlakeFargateHelper

from airflow.models.baseoperator import BaseOperator

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

class StarlakeAirflowFargateJob(StarlakeAirflowJob):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, options: Optional[dict] = None, **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.aws_conn_id = kwargs.get("aws_conn_id", self.caller_globals.get("aws_conn_id", __class__.get_context_var("aws_conn_id", "aws_default", options)))

    def sl_job(self, task_id: str, arguments: list, spark_config: Optional[StarlakeSparkConfig] = None, **kwargs) -> BaseOperator:
        """Overrides StarlakeAirflowJob.sl_job()
        Generate the Airflow task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.

        Returns:
            BaseOperator: The Airflow task.
        """
        fargate = StarlakeFargateHelper(job=self, arguments=arguments, **kwargs)

        overrides = kwargs.get("overrides", fargate.overrides)
        kwargs.pop("overrides", None)

        aws_conn_id = kwargs.get("aws_conn_id", self.aws_conn_id)
        kwargs.pop("aws_conn_id", None)

        network_configuration = kwargs.get("network_configuration", {
            "awsvpcConfiguration": {
                "subnets": fargate.subnets,
                "securityGroups": fargate.security_groups,
                "assignPublicIp": "DISABLED"
            }
        })
        kwargs.pop("network_configuration", None)

        return EcsRunTaskOperator(
            task_id=task_id,
            task_definition=fargate.task_definition,
            cluster=fargate.cluster,
            overrides=overrides,
            aws_conn_id=aws_conn_id,
            region=fargate.region,
            launch_type="FARGATE",
            network_configuration=network_configuration,
            wait_for_completion=True,
            **kwargs
        )

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.FARGATE
