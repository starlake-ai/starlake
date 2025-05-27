from __future__ import annotations

from ai.starlake.airflow.starlake_airflow_job import StarlakeAirflowJob, AirflowDataset, supports_inlet_events

from ai.starlake.common import sl_cron_start_end_dates, sl_scheduled_date, sl_scheduled_dataset, sl_timestamp_format

from ai.starlake.job import StarlakeOrchestrator, StarlakeExecutionMode

from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask

from airflow import DAG

from airflow.models.dag import DagContext

from airflow.datasets import Dataset

from airflow.models.baseoperator import BaseOperator

from airflow.utils.context import Context

from airflow.utils.task_group import TaskGroup, TaskGroupContext

from airflow.utils.state import DagRunState

from typing import Any, List, Optional, TypeVar, Union

J = TypeVar("J", bound=StarlakeAirflowJob)

class AirflowPipeline(AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset], AirflowDataset):
    def __init__(self, job: J, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, orchestration: Optional[AbstractOrchestration[DAG, BaseOperator, TaskGroup, Dataset]] = None, **kwargs) -> None:
        def fun(upstream: Union[BaseOperator, TaskGroup], downstream: Union[BaseOperator, TaskGroup]) -> None:
            downstream.set_upstream(upstream)

        super().__init__(job, orchestration_cls=AirflowOrchestration, dag=None, schedule=schedule, dependencies=dependencies, orchestration=orchestration, add_dag_dependency = fun, **kwargs)

        airflow_schedule: Union[str, List[Dataset], None] = None

        events = self.events

        # AssetOrTimeSchedule is not supported yet within SL
        if self.cron is not None:
            airflow_schedule = self.cron
        elif events:
            if not supports_inlet_events():
                airflow_schedule = events
            else:
                from functools import reduce
                airflow_schedule = reduce(lambda a, b: a | b, events)
                if self.job.data_cycle_enabled and not self.job.data_cycle:
                    self.job.data_cycle = self.computed_cron_expr
                    

        def ts_as_datetime(ts, context: Context = None):
            from datetime import datetime
            if not context:
                from airflow.operators.python import get_current_context
                context = get_current_context()
            ti = context["task_instance"]
            sl_logical_date = ti.xcom_pull(task_ids="start", key="sl_logical_date")
            if sl_logical_date:
                ts = sl_logical_date
            if isinstance(ts, str):
                from dateutil import parser
                import pytz
                return parser.isoparse(ts).astimezone(pytz.timezone('UTC'))
            elif isinstance(ts, datetime):
                return ts

        from datetime import datetime
        def sl_dates(cron_expr: str, start_time: datetime, context: Context = None) -> str:
            if not context:
                from airflow.operators.python import get_current_context
                context = get_current_context()
            ti = context["task_instance"]
            sl_start_date = ti.xcom_pull(task_ids="start", key="sl_previous_logical_date")
            sl_end_date = ti.xcom_pull(task_ids="start", key="sl_logical_date")
            if sl_start_date and sl_end_date:
                return f"sl_start_date='{sl_start_date.strftime(sl_timestamp_format)}',sl_end_date='{sl_end_date.strftime(sl_timestamp_format)}'"
            return sl_cron_start_end_dates(cron_expr, start_time, sl_timestamp_format)

        user_defined_macros = kwargs.get('user_defined_macros', job.caller_globals.get('user_defined_macros', dict()))
        kwargs.pop('user_defined_macros', None)
        user_defined_macros["sl_dates"] = sl_dates
        user_defined_macros["ts_as_datetime"] = ts_as_datetime
        user_defined_macros["sl_scheduled_dataset"] = sl_scheduled_dataset
        user_defined_macros["sl_scheduled_date"] = sl_scheduled_date

        user_defined_filters = kwargs.get('user_defined_filters', job.caller_globals.get('user_defined_filters', None))
        kwargs.pop('user_defined_filters', None)

        access_control = kwargs.get('access_control', job.caller_globals.get('access_control', None))
        kwargs.pop('access_control', None)

        self.dag = DAG(
            dag_id=self.pipeline_id, 
            schedule=airflow_schedule,
            catchup=self.catchup,
            tags=list(set([tag.upper() for tag in self.tags])), 
            default_args=job.caller_globals.get('default_dag_args', job.default_dag_args()),
            description=job.caller_globals.get('description', ""),
            start_date=job.start_date,
            end_date=job.end_date,
            user_defined_macros=user_defined_macros,
            user_defined_filters=user_defined_filters,
            access_control=access_control,
            **kwargs
        )

    def __enter__(self):
        DagContext.push_context_managed_dag(self.dag)
        return super().__enter__()
    
    def __exit__(self, exc_type, exc_value, traceback):
        DagContext.pop_context_managed_dag()
        return super().__exit__(exc_type, exc_value, traceback)

    def sl_transform_options(self, cron_expr: Optional[str] = None) -> Optional[str]:
        if cron_expr:
            return "{{sl_dates(params.cron_expr, ts_as_datetime(data_interval_end | ts))}}"
        return None

    def deploy(self, **kwargs) -> None:
        """Deploy the pipeline."""
        import os
        env = os.environ.copy() # Copy the current environment variables
        DAG_ID = self.pipeline_id
        AIRFLOW_HOME = kwargs.get('AIRFLOW_HOME', env.get('AIRFLOW_HOME', "/opt/airflow"))
        AIRFLOW_DAGS = f"{AIRFLOW_HOME}/dags"
        import shutil
        from pathlib import Path
        DAG_FILE = f"{AIRFLOW_DAGS}/{DAG_ID}.py"
        shutil.copyfile(Path(self.job.caller_globals['__file__']), Path(DAG_FILE))
        print(f"Pipeline {DAG_ID} deployed to {DAG_FILE}")

    def delete(self, **kwargs) -> None:
        """Delete the pipeline."""
        import os
        env = os.environ.copy() # Copy the current environment variables
        DAG_ID = self.pipeline_id
        AIRFLOW_BASE_URL = kwargs.get('AIRFLOW_BASE_URL', env.get('AIRFLOW_BASE_URL', "http://localhost:8080"))
        AIRFLOW_API_BASE_URL = f"{AIRFLOW_BASE_URL}/api/v1"
        AIRFLOW_USERNAME = kwargs.get('AIRFLOW_USERNAME', env.get('AIRFLOW_USERNAME', None))
        AIRFLOW_PASSWORD = kwargs.get('AIRFLOW_PASSWORD', env.get('AIRFLOW_PASSWORD', None))
        if AIRFLOW_USERNAME and AIRFLOW_PASSWORD:
            AIRFLOW_AUTH = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
        else:
            AIRFLOW_AUTH = None
        import requests
        response = requests.delete(
            f"{AIRFLOW_API_BASE_URL}/dags/{DAG_ID}",
            headers={'Content-Type': 'application/json'},
            auth=AIRFLOW_AUTH
        )
        response.raise_for_status()
        print(f"Pipeline {DAG_ID} deleted")

    def run(self, logical_date: Optional[str] = None, timeout: str = '120', mode: StarlakeExecutionMode = StarlakeExecutionMode.RUN, **kwargs) -> None:
        """Run the pipeline.
        Args:
            logical_date (Optional[str]): the logical date.
            timeout (str): the timeout in seconds.
            mode (StarlakeExecutionMode): the execution mode.
        """
        import os
        env = os.environ.copy() # Copy the current environment variables
        DAG_ID = self.pipeline_id
        if mode == StarlakeExecutionMode.DRY_RUN:
            # Test the pipeline with the given configuration
            from datetime import datetime
            import pendulum
            utc = pendulum.UTC
            execution_date = datetime.now(tz=utc)
            conf = dict()
            conf.update(kwargs)
            conf.update({'start_date': execution_date, 'backfill': False})
            from airflow.configuration import initialize_config
            initialize_config().load_test_config()
            try:
                print(f"Testing pipeline {DAG_ID} with execution date {execution_date} and  configuration {conf}")
                self.dag.test(execution_date=execution_date, run_conf=conf)
            except Exception as e:
                print(f"Pipeline {DAG_ID} failed with error {str(e)}")

        elif mode == StarlakeExecutionMode.RUN:
            import time
            # Run the pipeline with the given configuration
            AIRFLOW_BASE_URL = kwargs.get('AIRFLOW_BASE_URL', env.get('AIRFLOW_BASE_URL', "http://localhost:8080"))
            AIRFLOW_API_BASE_URL = f"{AIRFLOW_BASE_URL}/api/v1"
            AIRFLOW_USERNAME = kwargs.get('AIRFLOW_USERNAME', env.get('AIRFLOW_USERNAME', None))
            AIRFLOW_PASSWORD = kwargs.get('AIRFLOW_PASSWORD', env.get('AIRFLOW_PASSWORD', None))
            if AIRFLOW_USERNAME and AIRFLOW_PASSWORD:
                AIRFLOW_AUTH = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
            else:
                AIRFLOW_AUTH = None
            payload = {k: kwargs[k] for k in ['conf', 'logical_date', 'execution_date', 'dag_run_id'] if k in kwargs}
            # conf = kwargs.get('conf', {'backfill': True})
            # payload['conf'] = conf
            # generate a unique dag_run_id
            import uuid
            dag_run_id = f"manual_run_{uuid.uuid4()}"
            payload['dag_run_id'] = dag_run_id
            if logical_date:
                payload['logical_date'] = logical_date + 'Z'
                payload['execution_date'] = logical_date + 'Z'
            print(f"Starting pipeline {DAG_ID} with configuration {payload}")
            import requests
            from requests.exceptions import HTTPError
            response = requests.post(
                f"{AIRFLOW_API_BASE_URL}/dags/{DAG_ID}/dagRuns",
                headers={'Content-Type': 'application/json'},
                json=payload,
                auth=AIRFLOW_AUTH
            )
            try:
                response.raise_for_status()
            except HTTPError as e:
                print(f"Pipeline {DAG_ID} failed with error {str(e)}")
                return
            json_response: dict = response.json() or dict()
            dag_run_id = json_response.get('dag_run_id', None)
            if dag_run_id:
                print(f"Pipeline {DAG_ID} started with dag_run_id {dag_run_id}")
                def check_state() -> bool:
                    response = requests.get(
                        f"{AIRFLOW_API_BASE_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}",
                        headers={'Content-Type': 'application/json'},
                        auth=AIRFLOW_AUTH
                    )
                    response.raise_for_status()
                    json_response = response.json()
                    state = json_response.get('state', None)
                    if state == DagRunState.FAILED:
                        raise Exception(f"Pipeline {DAG_ID} failed")
                    elif state == DagRunState.SUCCESS:
                        print(f"Pipeline {DAG_ID} succeeded")
                        return True
                    elif state == DagRunState.QUEUED:
                        print(f"Pipeline {DAG_ID} is queued")
                        time.sleep(5)
                        return check_state()
                    elif state == DagRunState.RUNNING:
                        print(f"Pipeline {DAG_ID} is running")
                        time.sleep(5)
                        return check_state()
                    else:
                        print(f"Pipeline {DAG_ID} is in state {state}")
                        return False
                check_state()
            else:
                raise Exception(f"Pipeline {DAG_ID} failed")

        elif mode == StarlakeExecutionMode.BACKFILL:
            # Backfill the pipeline with the given configuration
            if not logical_date:
                raise ValueError("The logical date must be provided for backfilling")
            conf = kwargs.get('conf', {})
            conf['backfill'] = True
            kwargs.update({'conf': conf})
            self.run(logical_date=logical_date, timeout=timeout, mode=StarlakeExecutionMode.RUN, **kwargs)

        else:
            raise ValueError(f"Execution mode {mode} is not supported")

class AirflowTaskGroup(AbstractTaskGroup[TaskGroup]):
    def __init__(self, group_id: str, group: TaskGroup, **kwargs) -> None:
        super().__init__(group_id, orchestration_cls=AirflowOrchestration, group=group)

    def __enter__(self):
        TaskGroupContext.push_context_managed_task_group(self.group)
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        TaskGroupContext.pop_context_managed_task_group()
        return super().__exit__(exc_type, exc_value, traceback)

class AirflowOrchestration(AbstractOrchestration[DAG, BaseOperator, TaskGroup, Dataset]):
    def __init__(self, job: J, **kwargs) -> None:
        """Overrides AbstractOrchestration.__init__()
        Args:
            job (J): The job that will generate the tasks within the pipeline.
        """
        super().__init__(job, **kwargs) 

    @classmethod
    def sl_orchestrator(cls) -> str:
        return StarlakeOrchestrator.AIRFLOW

    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset]:
        """Create the Starlake pipeline to orchestrate.

        Args:
            schedule (Optional[StarlakeSchedule]): The optional schedule
            dependencies (Optional[StarlakeDependencies]): The optional dependencies
        
        Returns:
            AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset]: The pipeline to orchestrate.
        """
        return AirflowPipeline(
            self.job, 
            schedule, 
            dependencies, 
            self
        )

    def sl_create_task(self, task_id: str, task: Optional[Union[BaseOperator, TaskGroup]], pipeline: AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset]) -> Optional[Union[AbstractTask[BaseOperator], AbstractTaskGroup[TaskGroup]]]:
        if task is None:
            return None

        task.dag = pipeline.dag

        if isinstance(task, TaskGroup):
            task_group = AirflowTaskGroup(
                group_id = task.group_id.split('.')[-1],
                group = task, 
                dag = pipeline.dag,
            )

            with task_group:

                tasks = list(task.children.values())
                # sorted_tasks = []
                visited = {}

                def visit(t: Union[BaseOperator, TaskGroup]) -> Optional[Union[AbstractTask[BaseOperator], AbstractTaskGroup[TaskGroup]]]:
                    if isinstance(t, TaskGroup):
                        v_task_id = t.group_id
                    else:
                        v_task_id = t.task_id
                    if v_task_id in visited.keys():
                        return visited.get(v_task_id)
                    v = self.sl_create_task(v_task_id.split('.')[-1], t, pipeline)
                    visited.update({v_task_id: v})
                    for upstream in t.upstream_list:  # Visite récursive des tâches en amont
                        if upstream in tasks:
                            v_upstream = visit(upstream)
                            if v_upstream:
                                task_group.set_dependency(v_upstream, v)
                    # sorted_tasks.append(t)
                    return v

                for t in tasks:
                    visit(t)

            return task_group

        else:
            return AbstractTask(task_id, task)

    def sl_create_task_group(self, group_id: str, pipeline: AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset], **kwargs) -> AbstractTaskGroup[TaskGroup]:
        return AirflowTaskGroup(
            group_id, 
            group=TaskGroup(group_id=group_id, **kwargs),
            dag=pipeline.dag, 
            **kwargs
        )

    @classmethod
    def from_native(cls, native: Any) -> Optional[Union[AbstractTask[BaseOperator], AbstractTaskGroup[TaskGroup]]]:
        """Create a task or task group from a native object.
        Args:
            native (Any): the native object.
        Returns:
            Optional[Union[AbstractTask[BaseOperator], AbstractTaskGroup[TaskGroup]]]: the task or task group.
        """
        if isinstance(native, TaskGroup):
            return AirflowTaskGroup(native.group_id, native)
        elif isinstance(native, BaseOperator):
            return AbstractTask(native.task_id, native)
        else:
            return None
