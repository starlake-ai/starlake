from typing import Any, Dict, Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin

from ai.starlake.aws import StarlakeFargateHelper

from airflow.models.baseoperator import BaseOperator

from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.sensors.ecs import EcsTaskStateSensor
from airflow.providers.amazon.aws.hooks.ecs import EcsTaskStates

from airflow.sensors.base import PokeReturnValue

from airflow.utils.task_group import TaskGroup

import logging

class StarlakeAirflowFargateJob(StarlakeAirflowJob):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, options: Optional[dict] = None, **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.aws_conn_id = kwargs.get("aws_conn_id", self.caller_globals.get("aws_conn_id", __class__.get_context_var("aws_conn_id", "aws_default", self.options)))
        self.fargate_async = __class__.get_context_var(var_name='fargate_async', default_value="True", options=self.options).lower() == "true" 
        self.fargate_async_poke_interval = float(__class__.get_context_var('fargate_async_poke_interval', "30", self.options))
        self.retry_on_failure = __class__.get_context_var("retry_on_failure", "False", self.options).lower() == 'true'

    def sl_job(self, task_id: str, arguments: list, spark_config: Optional[StarlakeSparkConfig] = None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType] = None, **kwargs) -> BaseOperator:
        """Overrides StarlakeAirflowJob.sl_job()
        Generate the Airflow task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType], optional): The optional task type. Defaults to None.

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

        wait_for_completion = kwargs.get("wait_for_completion", not self.fargate_async)
        kwargs.pop("wait_for_completion", None)

        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if wait_for_completion:
            return FargateTaskOperator(
                task_id=task_id,
                dataset=dataset,
                source=self.source,
                task_definition=fargate.task_definition,
                cluster=fargate.cluster,
                overrides=overrides,
                aws_conn_id=aws_conn_id,
                region=fargate.region,
                launch_type="FARGATE",
                network_configuration=network_configuration,
                wait_for_completion=True,
                retry_on_failure=self.retry_on_failure,
                **kwargs
            )
        else:
            with TaskGroup(group_id=f"{task_id}_wait") as task_completion_sensors:
                run_task = FargateTaskOperator(
                    task_id=task_id,
                    dataset=None,
                    source=self.source,
                    task_definition=fargate.task_definition,
                    cluster=fargate.cluster,
                    overrides=overrides,
                    aws_conn_id=aws_conn_id,
                    region=fargate.region,
                    launch_type="FARGATE",
                    network_configuration=network_configuration,
                    wait_for_completion=False,
                    **kwargs
                )
                check_completion_id = task_id + '_check_completion'
                completion_sensor = FargateTaskStateSensor(
                    task_id=check_completion_id,
                    dataset=dataset,
                    source=self.source,
                    cluster=fargate.cluster,
                    task=run_task.output["ecs_task_arn"],
                    poke_interval=self.fargate_async_poke_interval,
                    pool=kwargs.get('pool', self.pool),
                )
                run_task >> completion_sensor
            return task_completion_sensors

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.FARGATE

class FargateTaskOperator(StarlakeDatasetMixin, EcsRunTaskOperator):
    def __init__(
        self,
        task_id: str,
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        task_definition: str,
        cluster: str,
        overrides: Dict[str, Any],
        aws_conn_id: str = 'aws_default',
        region: Optional[str] = None,
        launch_type: Optional[str] = None,
        network_configuration: Optional[Dict[str, Any]] = None,
        wait_for_completion: bool = True,
        retry_on_failure: bool = False,
        **kwargs
    ) -> None:
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            task_definition=task_definition,
            cluster=cluster,
            overrides=overrides,
            aws_conn_id=aws_conn_id,
            region=region,
            launch_type=launch_type,
            network_configuration=network_configuration,
            wait_for_completion=wait_for_completion,
            **kwargs
        )
        self.retry_on_failure = retry_on_failure

    def execute(self, context):
        logger = logging.getLogger(__name__)
        logger.info(f"Running fargate task {self.task_id}")
        try:
            super().execute(context)
            if self.wait_for_completion:
                return True
            else:
                return None
        except Exception as e:
            logger.exception(msg = f"Task {self.task_id} has failed")
            if self.wait_for_completion and self.do_xcom_push:
                self.xcom_push(context, key="return_value", value=False)
            if self.retry_on_failure:
                raise e

class FargateTaskStateSensor(StarlakeDatasetMixin, EcsTaskStateSensor):
    """
    This sensor waits until the ECS Task has completed by providing the target_state and failure_states parameters.
    """
    def __init__(
        self,
        task_id: str,
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        cluster: str,
        task: str,
        **kwargs
    ) -> None:
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            cluster=cluster,
            task=task,
            target_state=EcsTaskStates.STOPPED,
            failure_states={EcsTaskStates.NONE},
            **kwargs
        )

    def poke(self, context):
        logger = logging.getLogger(__name__)
        logger.info(f"Checking task {self.task} state")
        try:
            tasks = self.hook.conn.describe_tasks(cluster=self.cluster, tasks=[self.task]).get("tasks", [])
            if tasks:
                task = tasks[0]
                status: str = task.get("lastStatus", None)
                if status:
                    logger.info(f"Task {self.task} state: {status}")
                    if EcsTaskStates(status) in self.failure_states:
                        logger.error(msg = f"Task {self.task} has failed with status {status}")
                        return PokeReturnValue(True, False)
                    elif EcsTaskStates(status) == self.target_state:
                        containers = task.get("containers", [])
                        if containers and containers[0].get("exitCode", 1) == 0:
                            logger.info(f"Task {self.task} has succeeded")
                            return PokeReturnValue(True, True)
                        else:
                            logger.error(msg = f"Task {self.task} has failed")
                            return PokeReturnValue(True, False)
                else:
                    logger.error(msg = f"Task {self.task} has failed with no status")
                    return PokeReturnValue(True, False)
            return None
        except Exception as e:
            logger.error(msg = f"Task {self.task} has failed")
            return PokeReturnValue(True, False)
