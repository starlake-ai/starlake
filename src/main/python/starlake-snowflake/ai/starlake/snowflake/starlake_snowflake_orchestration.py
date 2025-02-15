from __future__ import annotations

from ai.starlake.job import StarlakeOrchestrator

from ai.starlake.dataset import str
from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask, AbstractDependency

from ai.starlake.snowflake.starlake_snowflake_job import StarlakeSnowflakeJob
from snowflake.core.task import Cron
from snowflake.core.task.dagv1 import DAG, DAGTask, _dag_context_stack

from typing import Any, List, Optional, Union

class SnowflakePipeline(AbstractPipeline[DAG, str], str):
    def __init__(self, job: StarlakeSnowflakeJob, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, orchestration: Optional[AbstractOrchestration[DAG, DAGTask, List[DAGTask], str]] = None, **kwargs) -> None:
        super().__init__(job, orchestration_cls=SnowflakeOrchestration, dag=None, schedule=schedule, dependencies=dependencies, orchestration=orchestration, **kwargs)

        snowflake_schedule: Union[Cron, None] = None
        if self.cron is not None:
            snowflake_schedule = Cron(self.cron, "Europe/Paris")
        elif self.datasets is not None:
            # TODO add the logic to convert the starlake datasets to snowflake streams
            ...

        self.dag = DAG(
            name=self.pipeline_id,
            schedule=snowflake_schedule,
            warehouse=job.warehouse,
            comment=job.caller_globals.get('description', ""),
            stage_location=job.stage_location,
            packages=job.packages,
        )
    def __enter__(self):
        _dag_context_stack.append(self.dag)
        return super().__enter__()
    
    def __exit__(self, exc_type, exc_value, traceback):
        _dag_context_stack.pop()

        # walk throw the dag to add the dependencies

        def get_node(dependency: AbstractDependency) -> Union[DAGTask, List[DAGTask], None]:
            if isinstance(dependency, AbstractTaskGroup):
                return dependency.group
            return dependency.task

        def update_group_dependencies(group: AbstractTaskGroup):
            def update_dependencies(upstream_dependencies, root_key):
                root = group.get_dependency(root_key)
                temp_root_node = get_node(root)
                if isinstance(temp_root_node, List[DAGTask]):
                    root_node = temp_root_node[-1] # get the last task
                else:
                    root_node = temp_root_node
                if isinstance(root, AbstractTaskGroup) and root_key != group.group_id:
                    update_group_dependencies(root)
                if root_key in upstream_dependencies:
                    for key in upstream_dependencies[root_key]:
                        downstream = group.get_dependency(key)
                        temp_downstream_node = get_node(downstream)
                        if isinstance(downstream, AbstractTaskGroup) and key != group.group_id:
                            update_group_dependencies(downstream)
                        if isinstance(temp_downstream_node, List[DAGTask]):
                            downstream_node = temp_downstream_node[0] # get the first task
                        else:
                            downstream_node = temp_downstream_node
                        downstream_node.add_predecessors(root_node)
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

class SnowflakeTaskGroup(AbstractTaskGroup[List[DAGTask]]):
    def __init__(self, group_id: str, group: List[DAGTask], dag: Optional[DAG] = None, **kwargs) -> None:
        super().__init__(group_id=group_id, orchestration_cls=SnowflakeOrchestration, group=group, **kwargs)
        self.dag = dag

    def __exit__(self, exc_type, exc_value, traceback):
        return super().__exit__(exc_type, exc_value, traceback)

class SnowflakeOrchestration(AbstractOrchestration[DAG, DAGTask, List[DAGTask], str]):
    def __init__(self, job: StarlakeSnowflakeJob, **kwargs) -> None:
        """Overrides AbstractOrchestration.__init__()
        Args:
            job (StarlakeSnowflakeJob): The Snowflake job that will generate the tasks within the pipeline.
        """
        super().__init__(job, **kwargs) 

    @classmethod
    def sl_orchestrator(cls) -> str:
        return StarlakeOrchestrator.SNOWFLAKE

    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[DAG, str]:
        """Create the Starlake pipeline to orchestrate.

        Args:
            schedule (Optional[StarlakeSchedule]): The optional schedule
            dependencies (Optional[StarlakeDependencies]): The optional dependencies
        
        Returns:
            AbstractPipeline[DAG, str]: The pipeline to orchestrate.
        """
        return SnowflakePipeline(
            self.job, 
            schedule, 
            dependencies, 
            self
        )

    def sl_create_task(self, task_id: str, task: Optional[Union[DAGTask, List[DAGTask]]], pipeline: AbstractPipeline[DAG, str]) -> Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]:
        if task is None:
            return None

        elif isinstance(task, list):
            task_group = SnowflakeTaskGroup(
                group_id = task[0].name.split('.')[-1],
                group = task, 
                dag = pipeline.dag,
            )

            with task_group:

                tasks = task
                # sorted_tasks = []

                visited = {}

                def visit(t: Union[DAGTask, List[DAGTask]]) -> Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]:
                    if isinstance(t, List[DAGTask]):
                        v_task_id = t[0].name.split('.')[-1]
                    else:
                        v_task_id = t.name
                    if v_task_id in visited.keys():
                        return visited.get(v_task_id)
                    v = self.sl_create_task(v_task_id.split('.')[-1], t, pipeline)
                    visited.update({v_task_id: v})
                    if isinstance(t, DAGTask):
                        for upstream in t.predecessors:  # Visite récursive des tâches en amont
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
            task.dag = pipeline.dag
            return AbstractTask(task_id, task)

    def sl_create_task_group(self, group_id: str, pipeline: AbstractPipeline[DAG, str], **kwargs) -> AbstractTaskGroup[List[DAGTask]]:
        return SnowflakeTaskGroup(
            group_id, 
            group=[],
            dag=pipeline.dag, 
            **kwargs
        )

    @classmethod
    def from_native(cls, native: Any) -> Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]:
        """Create a task or task group from a native object.
        Args:
            native (Any): the native object.
        Returns:
            Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]: the task or task group.
        """
        if isinstance(native, list):
            return SnowflakeTaskGroup(native[0].name.split('.')[-1], native)
        elif isinstance(native, DAGTask):
            return AbstractTask(native.task_id, native)
        else:
            return None
