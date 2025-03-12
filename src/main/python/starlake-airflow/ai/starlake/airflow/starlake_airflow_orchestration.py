from __future__ import annotations

from ai.starlake.airflow.starlake_airflow_job import StarlakeAirflowJob, AirflowDataset

from ai.starlake.common import sl_cron_start_end_dates, sl_scheduled_dataset

from ai.starlake.job import StarlakeOrchestrator, StarlakeExecutionMode

from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask, AbstractDependency

from airflow import DAG

from airflow.models.dag import DagContext

from airflow.datasets import Dataset

from airflow.models.baseoperator import BaseOperator

from airflow.utils.task_group import TaskGroup, TaskGroupContext

from typing import Any, List, Optional, TypeVar, Union

J = TypeVar("J", bound=StarlakeAirflowJob)

class AirflowPipeline(AbstractPipeline[DAG, BaseOperator, TaskGroup, Dataset], AirflowDataset):
    def __init__(self, job: J, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, orchestration: Optional[AbstractOrchestration[DAG, BaseOperator, TaskGroup, Dataset]] = None, **kwargs) -> None:
        super().__init__(job, orchestration_cls=AirflowOrchestration, dag=None, schedule=schedule, dependencies=dependencies, orchestration=orchestration, **kwargs)

        airflow_schedule: Union[str, List[Dataset], None] = None

        if self.cron is not None:
            airflow_schedule = self.cron
        elif self.events is not None:
            airflow_schedule = self.events

        def ts_as_datetime(ts):
            # Convert ts to a datetime object
            from datetime import datetime
            return datetime.fromisoformat(ts)

        user_defined_macros = kwargs.get('user_defined_macros', job.caller_globals.get('user_defined_macros', dict()))
        kwargs.pop('user_defined_macros', None)
        user_defined_macros["sl_dates"] = sl_cron_start_end_dates
        user_defined_macros["ts_as_datetime"] = ts_as_datetime
        user_defined_macros["sl_scheduled_dataset"] = sl_scheduled_dataset

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

        # walk throw the dag to add the dependencies

        def get_node(dependency: AbstractDependency) -> BaseOperator:
            if isinstance(dependency, AbstractTaskGroup):
                return dependency.group
            return dependency.task

        def update_group_dependencies(group: AbstractTaskGroup):
            def update_dependencies(upstream_dependencies, root_key):
                root = group.get_dependency(root_key)
                root_node: BaseOperator = get_node(root)
                if isinstance(root, AbstractTaskGroup) and root_key != group.group_id:
                    update_group_dependencies(root)
                if root_key in upstream_dependencies:
                    for key in upstream_dependencies[root_key]:
                        downstream = group.get_dependency(key)
                        downstream_node: BaseOperator = get_node(downstream)
                        if isinstance(downstream, AbstractTaskGroup) and key != group.group_id:
                            update_group_dependencies(downstream)
                        downstream_node.set_upstream(root_node)
                        update_dependencies(upstream_dependencies, key)

            upstream_dependencies = group.upstream_dependencies
            upstream_keys = upstream_dependencies.keys()
            downstream_keys = group.downstream_dependencies.keys()
            root_keys = upstream_keys - downstream_keys

            if not root_keys and len(upstream_keys) == 0 and len(downstream_keys) == 0:
                root_keys = group.dependencies_dict.keys()

            for root_key in root_keys:
                update_dependencies(upstream_dependencies, root_key)

        update_group_dependencies(self)

        return super().__exit__(exc_type, exc_value, traceback)

    def sl_transform_options(self, cron_expr: Optional[str] = None) -> Optional[str]:
        if cron_expr:
            return "{{sl_dates(params.cron_expr, ts_as_datetime(data_interval_end | ts))}}"
        return None

    def run(self, options: dict = dict(), mode: StarlakeExecutionMode = StarlakeExecutionMode.RUN) -> None:
        import os
        env = os.environ.copy() # Copy the current environment variables
        import json
        conf = json.dumps(options)
        if mode == StarlakeExecutionMode.DRY_RUN:
            import subprocess
            subprocess.run(
                args=f"airflow dags test {self.pipeline_id} --conf '{conf}'",
                env=env,
                check=True, 
                stderr=subprocess.STDOUT, 
                stdout=subprocess.PIPE,
            )
        elif mode == StarlakeExecutionMode.BACKFILL:
            import subprocess
            subprocess.run(
                args=f"airflow dags backfill {self.pipeline_id} --rerun-failed-tasks --conf '{conf}'",
                env=env,
                check=True, 
                stderr=subprocess.STDOUT, 
                stdout=subprocess.PIPE,
            )
        elif mode == StarlakeExecutionMode.RUN:
            import subprocess
            subprocess.run(
                args=f"airflow dags trigger {self.pipeline_id} --conf '{conf}'",
                env=env,
                check=True, 
                stderr=subprocess.STDOUT, 
                stdout=subprocess.PIPE,
            )

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
