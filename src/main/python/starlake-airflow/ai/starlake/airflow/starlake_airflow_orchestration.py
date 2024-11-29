from __future__ import annotations

from ai.starlake.airflow.starlake_airflow_job import StarlakeAirflowJob

from ai.starlake.common import sl_cron_start_end_dates

from ai.starlake.resource import StarlakeResource

from ai.starlake.orchestration import StarlakeOrchestration, StarlakeSchedule, StarlakeDependencies, StarlakePipeline, StarlakeTaskGroup

from airflow import DAG

from airflow.models.dag import DagContext

from airflow.datasets import Dataset

from airflow.models.baseoperator import BaseOperator

from airflow.utils.task_group import TaskGroup, TaskGroupContext

from typing import Generic, List, Optional, Set, TypeVar, Union

J = TypeVar("J", bound=StarlakeAirflowJob)

class StarlakeAirflowPipeline(Generic[J], StarlakePipeline[DAG, BaseOperator, Dataset, J]):
    def __init__(self, sl_job: J, sl_pipeline_id: str, sl_schedule: Optional[StarlakeSchedule] = None, sl_schedule_name: Optional[str] = None, sl_dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> None:
        if not isinstance(sl_job, StarlakeAirflowJob):
            raise TypeError(f"Expected an instance of StarlakeAirflowJob, got {type(sl_job).__name__}")
        super().__init__(sl_job, sl_pipeline_id, sl_schedule, sl_schedule_name, sl_dependencies, **kwargs)

        schedule: Union[str, List[Dataset], None] = None

        if self.sl_cron is not None:
            schedule = self.sl_cron
        elif self.sl_events is not None:
            schedule = self.sl_events

        def ts_as_datetime(ts):
            # Convert ts to a datetime object
            from datetime import datetime
            return datetime.fromisoformat(ts)

        user_defined_macros = kwargs.get('user_defined_macros', sl_job.caller_globals.get('user_defined_macros', dict()))
        kwargs.pop('user_defined_macros', None)
        user_defined_macros["sl_dates"] = sl_cron_start_end_dates
        user_defined_macros["ts_as_datetime"] = ts_as_datetime

        user_defined_filters = kwargs.get('user_defined_filters', sl_job.caller_globals.get('user_defined_filters', None))
        kwargs.pop('user_defined_filters', None)

        self.dag = DAG(
            dag_id=sl_pipeline_id, 
            schedule=schedule,
            catchup=self.sl_catchup,
            tags=list(set([tag.upper() for tag in self.sl_tags])), 
            default_args=sl_job.caller_globals.get('default_dag_args', sl_job.default_dag_args()),
            description=sl_job.caller_globals.get('description', ""),
            start_date=sl_job.start_date,
            end_date=sl_job.end_date,
            user_defined_macros=user_defined_macros,
            user_defined_filters=user_defined_filters,
            **kwargs
        )

        # Dynamically bind pipeline methods to DAG
        for attr_name in dir(self):
            if (attr_name == '__enter__' or attr_name == '__exit__' or not attr_name.startswith('__')) and callable(getattr(self, attr_name)):
                setattr(self.dag, attr_name, getattr(self, attr_name))

    def __enter__(self):
        DagContext.push_context_managed_dag(self.dag)
        return self.dag
    
    def __exit__(self, exc_type, exc_value, traceback):
        DagContext.pop_context_managed_dag()

    def sl_create_task_group(self, group_id: str, **kwargs) -> StarlakeTaskGroup[BaseOperator]:
        return StarlakeAirflowTaskGroup(group_id, dag=self.dag, **kwargs)

    def sl_start(self, **kwargs) -> BaseOperator:
        start = self.sl_job.dummy_op(task_id="start", dag=self.dag)
        schedule = self.sl_schedule
        schedule_name = self.sl_schedule_name
        if schedule:
            start.sl_outputs = [f"{domain.name}_{schedule_name}" if schedule_name else domain.name for domain in schedule.domains]
        return start

    def sl_end(self, output_resources: Optional[List[StarlakeResource]] = None, **kwargs) -> BaseOperator:
        pipeline_id = self.sl_pipeline_id
        outlets = list(map(lambda resource: self.to_event(resource=resource, source=pipeline_id), output_resources or []))
        end = self.sl_job.dummy_op(
            task_id="end", 
            outlets=outlets, 
            dag=self.dag
        )
        schedule = self.sl_schedule
        schedule_name = self.sl_schedule_name
        if schedule:
            end.sl_inputs = [f"{domain.name}_{schedule_name}" if schedule_name else domain.name for domain in schedule.domains]
        return end

    def sl_add_dependency(self, pipeline_upstream: Union[TaskGroup, BaseOperator], pipeline_downstream: Union[TaskGroup, BaseOperator], **kwargs) -> BaseOperator:
        return pipeline_upstream >> pipeline_downstream

    def get_sl_transform_options(self, cron_expr: Optional[str] = None) -> Optional[str]:
        if cron_expr:
            return "{{sl_dates(params.cron_expr, ts_as_datetime(data_interval_end | ts))}}"
        return None

    @classmethod
    def to_event(cls, resource: StarlakeResource, source: Optional[str] = None) -> Dataset:
        extra = {}
        if source:
            extra["source"] = source
        return Dataset(resource.url, extra)

class StarlakeAirflowTaskGroup(StarlakeTaskGroup[BaseOperator]):
    def __init__(self, group_id: str, **kwargs) -> None:
        super().__init__(group_id, **kwargs)
        self.task_group = TaskGroup(group_id=group_id, **kwargs)
        # Dynamically bind task group methods to TaskGroup
        for attr_name in dir(self):
            if (attr_name == '__enter__' or attr_name == '__exit__' or not attr_name.startswith('__')) and callable(getattr(self, attr_name)):
                setattr(self.task_group, attr_name, getattr(self, attr_name))

    def __enter__(self):
        TaskGroupContext.push_context_managed_task_group(self.task_group)
        return self.task_group

    def __exit__(self, exc_type, exc_value, traceback):
        TaskGroupContext.pop_context_managed_task_group()

class StarlakeAirflowOrchestration(Generic[J], StarlakeOrchestration[DAG, BaseOperator, Dataset, J]):
    def __init__(self, job: J, **kwargs) -> None:
        """Overrides StarlakeOrchestration.__init__()
        Args:
            job (J): The job to orchestrate.
        """
        if not isinstance(job, StarlakeAirflowJob):
            raise TypeError(f"Expected an instance of StarlakeAirflowJob, got {type(job).__name__}")
        super().__init__(job, **kwargs) 

    def sl_create_schedule_pipeline(self, sl_schedule: StarlakeSchedule, nb_schedules: int = 1, **kwargs) -> StarlakePipeline[DAG, BaseOperator, Dataset]:
        """Create the Starlake pipeline that will generate the DAG to orchestrate the load of the specified domains.

        Args:
            schedule (StarlakeSchedule): The required schedule
        
        Returns:
            DAG: The generated dag.
        """
        sl_job = self.job

        pipeline_name = sl_job.caller_filename.replace(".py", "").replace(".pyc", "").lower()

        if nb_schedules > 1:
            sl_pipeline_id = f"{pipeline_name}_{schedule.name}"
            sl_schedule_name = schedule.name
        else:
            sl_pipeline_id = pipeline_name
            sl_schedule_name = None

        return StarlakeAirflowPipeline(
            sl_job, 
            sl_pipeline_id, 
            sl_schedule, 
            sl_schedule_name,
        )

    def sl_create_dependencies_pipeline(self, dependencies: StarlakeDependencies, **kwargs) -> StarlakePipeline[DAG, BaseOperator, Dataset]:
        """Create the Starlake pipeline that will generate the dag to orchestrate the specified tasks.

        Args:
            dependencies (StarlakeDependencies): The required dependencies
        
        Returns:
            DAG: The generated dag.
        """
        sl_job = self.job

        return StarlakeAirflowPipeline(
            sl_job, 
            sl_pipeline_id = sl_job.caller_filename.replace(".py", "").replace(".pyc", "").lower(), 
            sl_dependencies = dependencies, 
            **kwargs
        )
