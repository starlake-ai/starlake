import os

from datetime import timedelta

import logging

from typing import Any, Dict, Optional, Sequence, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin

from ai.starlake.airflow.bash import StarlakeBashOperator

from airflow.exceptions import AirflowException

from airflow.models.baseoperator import BaseOperator

from airflow.operators.bash import BashOperator

from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook
from airflow.providers.google.cloud.operators.cloud_run import  CloudRunExecuteJobOperator

from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.sensors.bash import BashSensor

from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup

from google.cloud.run_v2.types import Execution
from google.longrunning import operations_pb2

from enum import Enum
CloudRunMode = Enum("CloudRunMode", ["SYNC", "DEFER", "ASYNC"])

class StarlakeAirflowCloudRunJob(StarlakeAirflowJob):
    """Airflow Starlake Cloud Run Job."""
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
            cloud_run_async:bool=None,
            cloud_run_async_poke_interval: float=None,
            retry_on_failure: bool=None,
            retry_delay_in_seconds: float=None,
            separator:str = ' ',
            **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.project_id = __class__.get_context_var(var_name='cloud_run_project_id', default_value=os.getenv("GCP_PROJECT"), options=self.options) if not project_id else project_id
        self.cloud_run_job_name = __class__.get_context_var(var_name='cloud_run_job_name', options=self.options) if not cloud_run_job_name else cloud_run_job_name
        self.cloud_run_job_region = __class__.get_context_var('cloud_run_job_region', "europe-west1", self.options) if not cloud_run_job_region else cloud_run_job_region
        self.cloud_run_service_account = __class__.get_context_var(var_name='cloud_run_service_account', default_value="", options=self.options) if not cloud_run_service_account else cloud_run_service_account
        if self.cloud_run_service_account:
            self.impersonate_service_account = f"--impersonate-service-account {self.cloud_run_service_account}"
        else:
            self.impersonate_service_account = ""
        self.cloud_run_async = __class__.get_context_var(var_name='cloud_run_async', default_value="True", options=self.options).lower() == "true" if cloud_run_async is None else cloud_run_async
        self.cloud_run_async_poke_interval = float(__class__.get_context_var('cloud_run_async_poke_interval', "30", self.options)) if not cloud_run_async_poke_interval else cloud_run_async_poke_interval
        self.separator = separator if separator != ',' else ' '
        self.update_env_vars = self.separator.join([(f"--update-env-vars \"^{self.separator}^" if i == 0 else "") + f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())]) + "\""
        self.retry_on_failure = __class__.get_context_var("retry_on_failure", "False", self.options).lower() == 'true' if retry_on_failure is None else retry_on_failure
        self.retry_delay_in_seconds = float(__class__.get_context_var("retry_delay_in_seconds", "10", self.options)) if retry_delay_in_seconds is None else retry_delay_in_seconds
        self.use_gcloud = __class__.get_context_var("use_gcloud", "True", self.options).lower() == 'true'

    @classmethod
    def sl_execution_environment(self) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.CLOUD_RUN

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]=None, task_type: Optional[TaskType] = None, **kwargs) -> BaseOperator:
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
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        kwargs.update({'retry_delay': timedelta(seconds=self.retry_delay_in_seconds)})
        command = f'^{self.separator}^' + self.separator.join(arguments)
        if self.cloud_run_async: # asynchronous job
            with TaskGroup(group_id=f'{task_id}_wait') as task_completion_sensors:
                if self.use_gcloud: # use gcloud
                    kwargs.update({'do_xcom_push': True})
                    job_task = BashOperator(
                        task_id=task_id,
                        bash_command=(
                            f"gcloud beta run jobs execute {self.cloud_run_job_name} "
                            f"--args \"{command}\" "
                            f"{self.update_env_vars} "
                            f"--async --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)' {self.impersonate_service_account}" #--task-timeout 300
                        ),
                        **kwargs
                    )
                    # check job completion
                    check_completion_id = task_id + '_check_completion'
                    completion_sensor = GCloudRunJobCompletionSensor(
                        task_id=check_completion_id,
                        dataset=dataset if self.retry_on_failure else None,
                        source=self.source,
                        project_id=self.project_id,
                        cloud_run_job_region=self.cloud_run_job_region,
                        source_task_id=job_task.task_id,
                        retry_on_failure=self.retry_on_failure,
                        poke_interval=self.cloud_run_async_poke_interval,
                        impersonate_service_account = self.impersonate_service_account,
                        **kwargs
                    )
                    if self.retry_on_failure:
                        job_task >> completion_sensor
                    else:
                        # check job status
                        get_completion_status_id = task_id + '_get_completion_status'
                        source_task_id=job_task.task_id
                        bash_command = (f"value=`gcloud beta run jobs executions describe {{{{task_instance.xcom_pull(key=None, task_ids='{source_task_id}')}}}} --region {self.cloud_run_job_region} --project {self.project_id} --format='value(status.failedCount, status.cancelledCounts)' {self.impersonate_service_account}| sed 's/[[:blank:]]//g'`; test -z \"$value\"")
                        if kwargs.get('do_xcom_push', False):
                            bash_command=f"""
                            set -e
                            bash -c '
                            {bash_command}
                            return_code=$?

                            # Push the return code to XCom
                            echo $return_code

                            # Exit with the captured return code if non-zero
                            if [ $return_code -ne 0 ]; then
                                exit $return_code
                            fi
                            '
                            """
                        job_status = StarlakeBashOperator(
                            task_id=get_completion_status_id,
                            dataset=dataset,
                            source=self.source,
                            bash_command=bash_command,
                            **kwargs
                        )
                        job_task >> completion_sensor >> job_status

                else:
                    container_overrides: Dict[str, Any] = {
                        "env": [
                            {"name": key, "value": value} for key, value in self.sl_env_vars.items()
                        ]
                    }
                    container_overrides["args"] = arguments
                    job_overrides = {"container_overrides": [container_overrides]}
                    job_task = CloudRunJobOperator(
                        task_id=task_id,
                        dataset=None,
                        source=self.source,
                        project_id=self.project_id,
                        job_name=self.cloud_run_job_name,
                        region=self.cloud_run_job_region,
                        overrides=job_overrides,
                        mode=CloudRunMode.ASYNC,
                        impersonation_chain=self.impersonate_service_account,
                        **kwargs
                    )
                    check_completion_id = task_id + '_check_completion'
                    completion_sensor = CloudRunJobCompletionSensor(
                        task_id=check_completion_id,
                        dataset=dataset,
                        source=self.source,
                        source_task_id=job_task.task_id,
                        impersonation_chain=self.impersonate_service_account,
                        **kwargs
                    )

                    job_task >> completion_sensor

            return task_completion_sensors

        else: # synchronous job
            if self.use_gcloud:
                bash_command = (
                    f"gcloud beta run jobs execute {self.cloud_run_job_name} "
                    f"--args \"{command}\" "
                    f"{self.update_env_vars} "
                    f"--wait --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)' {self.impersonate_service_account}" #--task-timeout 300 
                )
                if kwargs.get('do_xcom_push', False):
                    bash_command=f"""
                    set -e
                    bash -c '
                    {bash_command}
                    return_code=$?

                    # Push the return code to XCom
                    echo $return_code

                    # Exit with the captured return code if non-zero
                    if [ $return_code -ne 0 ]; then
                        exit $return_code
                    fi
                    '
                    """
                kwargs.pop('do_xcom_push', None)
                return StarlakeBashOperator(
                    task_id=task_id,
                    dataset=dataset,
                    source=self.source,
                    bash_command=bash_command,
                    do_xcom_push=True,
                    **kwargs
                )
            else:
                container_overrides: Dict[str, Any] = {
                    "env": [
                        {"name": key, "value": value} for key, value in self.sl_env_vars.items()
                    ]
                }
                container_overrides["args"] = arguments
                job_overrides = {"container_overrides": [container_overrides]}
                return CloudRunJobOperator(
                    task_id=task_id,
                    dataset=dataset,
                    source=self.source,
                    project_id=self.project_id,
                    job_name=self.cloud_run_job_name,
                    region=self.cloud_run_job_region,
                    overrides=job_overrides,
                    mode=CloudRunMode.SYNC,
                    impersonation_chain=self.impersonate_service_account,
                    **kwargs
                )

class GCloudRunJobCompletionSensor(StarlakeDatasetMixin, BashSensor):
    '''
    This sensor checks the completion of a cloud run job using gcloud.
    '''
    def __init__(self, 
                 *, 
                 task_id: str, 
                 dataset: Optional[Union[StarlakeDataset, str]],
                 source: Optional[str],
                 project_id: str, 
                 cloud_run_job_region: str, 
                 source_task_id: str, 
                 retry_on_failure: bool=None, 
                 impersonate_service_account: str=None, 
                 **kwargs
        ) -> None:
        if retry_on_failure:
            kwargs.update({'retry_exit_code': 2})
            bash_command = (
                "check_completion=`gcloud beta run jobs executions describe "
                f"{{{{ task_instance.xcom_pull(key='return_value', task_ids='{source_task_id}') }}}} "
                f"--region {cloud_run_job_region} "
                f"--project {project_id} "
                "--format='value(status.completionTime, status.cancelledCounts)' "
                "| sed 's/[[:blank:]]//g'`; "
                "if [ -z \"$check_completion\" ]; then exit 2; else "
                "check_status=`gcloud beta run jobs executions describe "
                f"{{{{ task_instance.xcom_pull(key='return_value', task_ids='{source_task_id}') }}}} "
                f"--region {cloud_run_job_region} "
                f"--project {project_id} "
                f"--format='value(status.failedCount, status.cancelledCounts)' {impersonate_service_account}"
                "| sed 's/[[:blank:]]//g'`; "
                "test -z \"$check_status\" && exit 0 || exit 1; fi"
            )
        else:
            bash_command=(f"value=`gcloud beta run jobs executions describe {{{{task_instance.xcom_pull(key='return_value', task_ids='{source_task_id}')}}}}  --region {cloud_run_job_region} --project {project_id} --format='value(status.completionTime, status.cancelledCounts)' {impersonate_service_account}| sed 's/[[:blank:]]//g'`; test -n \"$value\"")

        if kwargs.get('do_xcom_push', False) and retry_on_failure:
            bash_command=f"""
            set -e
            bash -c '
            {bash_command}
            return_code=$?

            # Push the return code to XCom
            echo $return_code

            # Exit with the captured return code if non-zero
            if [ $return_code -ne 0 ]; then
                exit $return_code
            fi
            '
            """
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            bash_command=bash_command,
            mode="reschedule",
            **kwargs
        )

class CloudRunJobOperator(StarlakeDatasetMixin, CloudRunExecuteJobOperator):
    """
    This extends official CloudRunExecuteJobOperator in order to implement asynchronous job.
    """

    def __init__(
        self,
        task_id: str, 
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        mode: CloudRunMode = CloudRunMode.SYNC,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Union[str, Sequence[str], None] = None,
        **kwargs,
    ):
        super().__init__(  # type: ignore
            task_id=task_id,
            dataset=dataset,
            source=source,
            gcp_conn_id=gcp_conn_id, 
            impersonation_chain=impersonation_chain, 
            **kwargs
        )
        self.mode = mode

    def execute(self, context: Context):
        logger = logging.getLogger(__name__)
        if self.mode == CloudRunMode.ASYNC:
            hook: CloudRunHook = CloudRunHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
            self.operation = hook.execute_job(
                region=self.region,
                project_id=self.project_id,
                job_name=self.job_name,
                overrides=self.overrides,
            )
            execution = Execution.deserialize(self.operation.operation.metadata.value)
            job_id = execution.name.split("/")[-1]
            logger.info(
                f"https://console.cloud.google.com/run/jobs/executions/details/{self.region}/{job_id}/tasks?project={self.project_id}"
            )
            logger.info(execution.log_uri)
            return self.operation.operation.name

        else:
            try:
                job = super(CloudRunJobOperator, self).execute(context)
                if self.do_xcom_push:
                    self.xcom_push(context, key="job", value=job)
                return True
            except Exception as e:
                logger.exception(msg=f"Task {self.task_id} has failed")
                return False

class CloudRunJobCompletionSensor(StarlakeDatasetMixin, BaseSensorOperator):

    template_fields = ("gcp_conn_id", "impersonation_chain")

    def __init__(
        self,
        *,
        task_id: str, 
        dataset: Optional[Union[StarlakeDataset, str]],
        source: Optional[str],
        source_task_id: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Union[str, Sequence[str], None] = None,
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            dataset=dataset,
            source=source,
            mode="reschedule", 
            **kwargs
        )
        self.source_task_id = source_task_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context):
        hook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        operation_name = self.xcom_pull(context, task_ids=self.source_task_id)
        operation_request = operations_pb2.GetOperationRequest(name=operation_name)
        operation: operations_pb2.Operation = hook.get_conn().get_operation(
            operation_request
        )
        if operation.done:
            # An operation can only have one of those two combinations: if it is failed, then
            # the error field will be populated, else, then the response field will be.
            if operation.error.SerializeToString():
                if self.do_xcom_push:
                    self.log.error(
                        f"{operation.error.message} [{operation.error.code}]"
                    )
                    return PokeReturnValue(True, False)
                else:
                    raise AirflowException(
                        f"{operation.error.message} [{operation.error.code}]"
                    )
            return PokeReturnValue(True, True)
        return PokeReturnValue(False, False)
