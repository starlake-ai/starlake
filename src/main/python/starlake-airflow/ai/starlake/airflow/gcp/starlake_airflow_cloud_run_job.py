import os

from datetime import timedelta

from typing import Union

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig

from ai.starlake.airflow import StarlakeAirflowJob

from airflow.models.baseoperator import BaseOperator

from airflow.operators.bash import BashOperator

from airflow.sensors.bash import BashSensor

from airflow.utils.task_group import TaskGroup

class StarlakeAirflowCloudRunJob(StarlakeAirflowJob):
    """Airflow Starlake Cloud Run Job."""
    def __init__(
            self,
            pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None,
            project_id: str=None,
            cloud_run_job_name: str=None,
            cloud_run_job_region: str=None,
            options: dict=None,
            cloud_run_async:bool=None,
            retry_on_failure: bool=None,
            retry_delay_in_seconds: float=None,
            separator:str = ' ',
            **kwargs):
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.project_id = __class__.get_context_var(var_name='cloud_run_project_id', default_value=os.getenv("GCP_PROJECT"), options=self.options) if not project_id else project_id
        self.cloud_run_job_name = __class__.get_context_var(var_name='cloud_run_job_name', options=self.options) if not cloud_run_job_name else cloud_run_job_name
        self.cloud_run_job_region = __class__.get_context_var('cloud_run_job_region', "europe-west1", self.options) if not cloud_run_job_region else cloud_run_job_region
        self.cloud_run_async = __class__.get_context_var(var_name='cloud_run_async', default_value="True", options=self.options).lower() == "true" if cloud_run_async is None else cloud_run_async
        self.separator = separator if separator != ',' else ' '
        self.update_env_vars = self.separator.join([(f"--update-env-vars \"^{self.separator}^" if i == 0 else "") + f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())]) + "\""
        self.retry_on_failure = __class__.get_context_var("retry_on_failure", "False", self.options).lower() == 'true' if retry_on_failure is None else retry_on_failure
        self.retry_delay_in_seconds = float(__class__.get_context_var("retry_delay_in_seconds", "10", self.options)) if retry_delay_in_seconds is None else retry_delay_in_seconds

    def __job_with_completion_sensors__(self, task_id: str, command: str, spark_config: StarlakeSparkConfig=None, **kwargs) -> TaskGroup:
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        with TaskGroup(group_id=f'{task_id}_wait') as task_completion_sensors:
            # asynchronous job
            job_task = BashOperator(
                task_id=task_id,
                bash_command=(
                    f"gcloud beta run jobs execute {self.cloud_run_job_name} "
                    f"--args \"{command}\" "
                    f"{self.update_env_vars} "
                    f"--async --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)'" #--task-timeout 300 
                ),
                do_xcom_push=True,
                **kwargs
            )
            # check job completion
            check_completion_id = task_id + '_check_completion'
            completion_sensor = CloudRunJobCompletionSensor(
                task_id=check_completion_id,
                project_id=self.project_id,
                cloud_run_job_region=self.cloud_run_job_region,
                source_task_id=job_task.task_id,
                retry_on_failure=self.retry_on_failure,
                **kwargs
            )
            if self.retry_on_failure:
                job_task >> completion_sensor
            else:
                # check job status
                get_completion_status_id = task_id + '_get_completion_status'
                job_status = CloudRunJobCheckStatusOperator(
                    task_id=get_completion_status_id,
                    project_id=self.project_id,
                    cloud_run_job_region=self.cloud_run_job_region,
                    source_task_id=job_task.task_id,
                    **kwargs
                )
                job_task >> completion_sensor >> job_status
        return task_completion_sensors

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
        """Overrides StarlakeAirflowJob.sl_job()
        Generate the Airflow task that will run the starlake command.
        
        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            
        Returns:
            BaseOperator: The Airflow task.
        """
        command = f'^{self.separator}^' + self.separator.join(arguments)
        if self.cloud_run_async:
            return self.__job_with_completion_sensors__(task_id=task_id, command=command, spark_config=spark_config, **dict(kwargs, **{'retry_delay': timedelta(seconds=self.retry_delay_in_seconds)}))
        else:
            # synchronous job
            return BashOperator(
                task_id=task_id,
                bash_command=(
                    f"gcloud beta run jobs execute {self.cloud_run_job_name} "
                    f"--args \"{command}\" "
                    f"{self.update_env_vars} "
                    f"--wait --region {self.cloud_run_job_region} --project {self.project_id} --format='get(metadata.name)'" #--task-timeout 300 
                ),
                do_xcom_push=True,
                **dict(kwargs, **{'retry_delay': timedelta(seconds=self.retry_delay_in_seconds)})
            )

class CloudRunJobCompletionSensor(BashSensor):
    '''
    This sensor checks the completion of a cloud run job.
    '''
    def __init__(self, *, project_id: str, cloud_run_job_region: str, source_task_id: str, retry_on_failure: bool=None, **kwargs) -> None:
        if retry_on_failure:
            super().__init__(
                bash_command=(
                    "check_completion=`gcloud beta run jobs executions describe "
                    f"{{{{ task_instance.xcom_pull(key=None, task_ids='{source_task_id}') }}}} "
                    f"--region {cloud_run_job_region} "
                    f"--project {project_id} "
                    "--format='value(status.completionTime, status.cancelledCounts)' "
                    "| sed 's/[[:blank:]]//g'`; "
                    "if [ -z \"$check_completion\" ]; then exit 2; else "
                    "check_status=`gcloud beta run jobs executions describe "
                    f"{{{{ task_instance.xcom_pull(key=None, task_ids='{source_task_id}') }}}} "
                    f"--region {cloud_run_job_region} "
                    f"--project {project_id} "
                    "--format='value(status.failedCount, status.cancelledCounts)' "
                    "| sed 's/[[:blank:]]//g'`; "
                    "test -z \"$check_status\" && exit 0 || exit 1; fi"
                ),
                mode="reschedule",
                retry_exit_code=2, #retry_exit_code requires airflow v2.6
                **kwargs
            )
        else:
            super().__init__(
                bash_command=(f"value=`gcloud beta run jobs executions describe {{{{task_instance.xcom_pull(key=None, task_ids='{source_task_id}')}}}}  --region {cloud_run_job_region} --project {project_id} --format='value(status.completionTime, status.cancelledCounts)' | sed 's/[[:blank:]]//g'`; test -n \"$value\""),
                mode="reschedule",
                **kwargs
            )

class CloudRunJobCheckStatusOperator(BashOperator):
    '''
    This operator checks the status of a cloud run job and fails if it is not successful.
    '''
    def __init__(self, *, project_id: str, cloud_run_job_region: str, source_task_id: str, **kwargs) -> None:
        super().__init__(
            bash_command=(f"value=`gcloud beta run jobs executions describe {{{{task_instance.xcom_pull(key=None, task_ids='{source_task_id}')}}}} --region {cloud_run_job_region} --project {project_id} --format='value(status.failedCount, status.cancelledCounts)' | sed 's/[[:blank:]]//g'`; test -z \"$value\""),
            **kwargs
        )
