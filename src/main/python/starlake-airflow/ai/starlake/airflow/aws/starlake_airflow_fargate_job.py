from typing import Any, Dict, Optional, Union

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment

from ai.starlake.airflow import StarlakeAirflowJob

from airflow.models.baseoperator import BaseOperator

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

class StarlakeAirflowFargateJob(StarlakeAirflowJob):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, options: Optional[dict] = None, **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.task_definition = kwargs.get("task_definition", self.caller_globals.get("aws_task_definition_name", __class__.get_context_var("aws_task_definition_name", None, options)))
        self.container_name = kwargs.get("container_name", self.caller_globals.get("aws_task_definition_container_name", __class__.get_context_var("aws_task_definition_container_name", None, options)))
        self.cluster = kwargs.get("cluster", self.caller_globals.get("aws_cluster_name", __class__.get_context_var("aws_cluster_name", None, options)))
        self.aws_conn_id = kwargs.get("aws_conn_id", self.caller_globals.get("aws_conn_id", __class__.get_context_var("aws_conn_id", "aws_default", options)))
        self.region = kwargs.get("region", self.caller_globals.get("aws_region", __class__.get_context_var("aws_region", "eu-west-3", options)))
        self.cpu = kwargs.get("cpu", self.caller_globals.get("cpu", __class__.get_context_var("cpu", 1024, options)))
        self.memory = kwargs.get("memory", self.caller_globals.get("memory", __class__.get_context_var("memory", 2048, options)))
        subnets = kwargs.get("subnets", self.caller_globals.get("aws_task_private_subnets", __class__.get_context_var("aws_task_private_subnets", [], options)))
        if isinstance(subnets, str):
            self.subnets = subnets.split(",")
        else:
            self.subnets = subnets
        security_groups = kwargs.get("security_groups", self.caller_globals.get("aws_task_security_groups", __class__.get_context_var("aws_task_security_groups", [], options).split(",")))
        if isinstance(security_groups, str):
            self.security_groups = security_groups.split(",")
        else:
            self.security_groups = security_groups

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
        task_definition = kwargs.get("task_definition", self.task_definition)
        kwargs.pop("task_definition", None)

        cluster = kwargs.get("cluster", self.cluster)
        kwargs.pop("cluster", None)

        container_overrides: Dict[str, Any] = {
            "name": kwargs.get("container_name", self.container_name),
            "command": arguments,
            "environment": [
                {"name": key, "value": value} for key, value in self.sl_env_vars.items()
            ],
            "cpu": kwargs.get("cpu", self.cpu),
            "memory": kwargs.get("memory", self.memory)
        }
        overrides = {"containerOverrides": [container_overrides]}
        kwargs.pop("container_name", None)
        kwargs.pop("cpu", None)
        kwargs.pop("memory", None)

        aws_conn_id = kwargs.get("aws_conn_id", self.aws_conn_id)
        kwargs.pop("aws_conn_id", None)

        region = kwargs.get("region", self.region)
        kwargs.pop("region", None)

        network_configuration = kwargs.get("network_configuration", {
            "awsvpcConfiguration": {
                "subnets": kwargs.get("subnets", self.subnets),
                "securityGroups": kwargs.get("security_groups", self.security_groups),
                "assignPublicIp": "DISABLED"
            }
        })
        kwargs.pop("network_configuration", None)
        kwargs.pop("subnets", None)
        kwargs.pop("security_groups", None)

        return EcsRunTaskOperator(
            task_id=task_id,
            task_definition=task_definition,
            cluster=cluster,
            overrides=overrides,
            aws_conn_id=aws_conn_id,
            region=region,
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
