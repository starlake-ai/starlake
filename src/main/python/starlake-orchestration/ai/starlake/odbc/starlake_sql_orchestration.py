from __future__ import annotations

from abc import ABC

from ai.starlake.job import StarlakeOrchestrator, StarlakeExecutionMode

from ai.starlake.dataset import StarlakeDataset
from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask, AbstractDependency

from ai.starlake.odbc import SessionFactory, SessionProvider, SQLTask

from ai.starlake.odbc.starlake_sql_job import StarlakeSQLJob

from typing import Any, List, Optional, Set, Union

from datetime import timedelta

class SQLDag(ABC):
    def __init__(
        self,
        name: str,
        *,
        schedule: Optional[Union[str, timedelta]] = None,
        comment: Optional[str] = None,
        computed_cron: Optional[str] = None,
        not_scheduled_datasets: Optional[List[StarlakeDataset]] = None,
        least_frequent_datasets: Optional[List[StarlakeDataset]] = None,
        most_frequent_datasets: Optional[List[StarlakeDataset]] = None,
    ) -> None:
        import logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        condition = None

        changes = dict() # tracks the datasets whose changes have to be checked

        if not schedule: # if the DAG is not scheduled we will rely on streams to trigger the underlying dag and check if the scheduled datasets without streams have data using CHANGES

            if least_frequent_datasets:
                self.logger.info(f"least frequent datasets: {','.join(list(map(lambda x: x.sink, least_frequent_datasets)))}")
                for dataset in least_frequent_datasets:
                    changes.update({dataset.sink: dataset.cron})

            not_scheduled_streams = set() # set of streams which underlying datasets are not scheduled
            not_scheduled_datasets_without_streams = []
            if not_scheduled_datasets:
                self.logger.info(f"not scheduled datasets: {','.join(list(map(lambda x: x.sink, not_scheduled_datasets)))}")
                for dataset in not_scheduled_datasets:
                    if dataset.stream:
                        not_scheduled_streams.add(dataset.stream)
                    else:
                        not_scheduled_datasets_without_streams.append(dataset)
            if not_scheduled_datasets_without_streams:
                self.logger.warning(f"Warning: No streams found for {','.join(list(map(lambda x: x.sink, not_scheduled_datasets_without_streams)))}")
                ... # nothing to do here

            if most_frequent_datasets:
                self.logger.info(f"most frequent datasets: {','.join(list(map(lambda x: x.sink, most_frequent_datasets)))}")
            streams = set()
            most_frequent_datasets_without_streams = []
            if most_frequent_datasets:
                for dataset in most_frequent_datasets:
                    if dataset.stream:
                        streams.add(dataset.stream)
                    else:
                        most_frequent_datasets_without_streams.append(dataset)
                        changes.update({dataset.sink: dataset.cron})
            if most_frequent_datasets_without_streams:
                self.logger.warning(f"Warning: No streams found for {','.join(list(map(lambda x: x.sink, most_frequent_datasets_without_streams)))}")
                ...

            if streams:
                condition = ' OR '.join(streams)

            if not_scheduled_streams:
                if condition:
                    condition = f"({condition}) AND ({' AND '.join(not_scheduled_streams)})"
                else:
                    condition = ' AND '.join(not_scheduled_streams)

        self.comment = comment
        self.name = name
        self.schedule = schedule
        self.computed_cron = computed_cron
        self.changes = changes
        self.condition = condition

class SQLPipeline(AbstractPipeline[SQLDag, SQLTask, List[SQLTask], StarlakeDataset], StarlakeDataset):
    def __init__(self, job: StarlakeSQLJob, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, orchestration: Optional[AbstractOrchestration[SQLDag, SQLTask, List[SQLTask], StarlakeDataset]] = None, **kwargs) -> None:
        super().__init__(job, orchestration_cls=SQLOrchestration, dag=None, schedule=schedule, dependencies=dependencies, orchestration=orchestration, **kwargs)

        sql_schedule: Union[str, None] = None
        computed_cron: Union[str, None] = None
        if self.cron is not None:
            sql_schedule = self.cron
        elif self.datasets is not None:
            if self.computed_cron_expr is not None:
                computed_cron = self.computed_cron_expr
            else:
                sql_schedule = timedelta(minutes=5)

        self.dag = SQLDag(
            name=self.pipeline_id,
            schedule=sql_schedule,
            comment=job.caller_globals.get('description', f"Pipeline {self.pipeline_id}"),
            computed_cron=computed_cron,
            not_scheduled_datasets=self.not_scheduled_datasets,
            least_frequent_datasets=self.least_frequent_datasets,
            most_frequent_datasets=self.most_frequent_datasets,
            # task_auto_retry_attempts=job.retries,            
        )

    def run(self, logical_date: Optional[str] = None, timeout: str = '120', mode: StarlakeExecutionMode = StarlakeExecutionMode.RUN, **kwargs) -> None:
        """Run the pipeline.
        Args:
            logical_date (Optional[str]): the logical date.
            timeout (str): the timeout in seconds.
            mode (StarlakeExecutionMode): the execution mode.
        """
        provider: Optional[str] = kwargs.get('provider', None)
        database: Optional[str] = kwargs.get('database', None)
        if provider is None:
            provider = SessionProvider.DUCKDB
            database = database or f"/tmp/{self.pipeline_id}.db"
        else:
            kwargs.pop('provider', None)
            provider = SessionProvider(provider)
        session = SessionFactory.session(provider=provider, database = database, **kwargs)
        config = dict()
        if logical_date:
            config.update({"logical_date": logical_date})
        cron_expr = self.computed_cron_expr
        if cron_expr:
            config.update({"cron_expr": cron_expr})

        if mode == StarlakeExecutionMode.DRY_RUN:
            for task in self.tasks:
                if isinstance(task, SQLTask):
                    task.execute(session, self.pipeline_id, config, True)

        elif mode == StarlakeExecutionMode.RUN:
            for task in self.tasks:
                if isinstance(task, SQLTask):
                    task.execute(session, self.pipeline_id, config, False)

        elif mode == StarlakeExecutionMode.BACKFILL:
            if not logical_date:
                raise ValueError("Logical date must be provided to backfill the pipeline")
            self.run(logical_date=logical_date, timeout=timeout, mode=StarlakeExecutionMode.RUN, **kwargs)

        else:
            raise ValueError(f"Execution mode {mode} is not supported")


class SQLTaskGroup(AbstractTaskGroup[List[SQLTask]]):
    def __init__(self, group_id: str, group: List[SQLTask], dag: Optional[SQLDag] = None, **kwargs) -> None:
        super().__init__(group_id=group_id, orchestration_cls=SQLOrchestration, group=group, **kwargs)
        self.dag = dag
        self.__group_as_map = dict()

    def __exit__(self, exc_type, exc_value, traceback):
        for dep in self.dependencies:
            if isinstance(dep, SQLTaskGroup):
                self.__group_as_map.update({dep.id: dep})
            elif isinstance(dep, AbstractTask):
                self.__group_as_map.update({dep.id: dep.task})
        return super().__exit__(exc_type, exc_value, traceback)

    @property
    def group_leaves(self) -> List[SQLTask]:
        leaves = []
        for leaf in self.leaves_keys:
            dep = self.__group_as_map.get(leaf, None)
            if dep:
                if isinstance(dep, SQLTaskGroup):
                    leaves.extend(dep.group_leaves)
                else:
                    leaves.append(dep)
        return leaves

    @property
    def group_roots(self) -> List[SQLTask]:
        roots = []
        for root in self.roots_keys:
            dep = self.__group_as_map.get(root, None)
            if dep:
                if isinstance(dep, SQLTaskGroup):
                    roots.extend(dep.group_roots)
                else:
                    roots.append(dep)
        return roots

class SQLOrchestration(AbstractOrchestration[SQLDag, SQLTask, List[SQLTask], StarlakeDataset]):
    def __init__(self, job: StarlakeSQLJob, **kwargs) -> None:
        """Overrides AbstractOrchestration.__init__()
        Args:
            job (StarlakeSnowflakeJob): The Snowflake job that will generate the tasks within the pipeline.
        """
        super().__init__(job, **kwargs) 

    @classmethod
    def sl_orchestrator(cls) -> str:
        return StarlakeOrchestrator.STARLAKE

    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[SQLDag, SQLTask, List[SQLTask], StarlakeDataset]:
        """Create the Starlake pipeline to orchestrate.

        Args:
            schedule (Optional[StarlakeSchedule]): The optional schedule
            dependencies (Optional[StarlakeDependencies]): The optional dependencies
        
        Returns:
            AbstractPipeline[SQLDag, SQLTask, List[SQLTask], StarlakeDataset]: The pipeline to orchestrate.
        """
        return SQLPipeline(
            self.job, 
            schedule, 
            dependencies, 
            self
        )

    def sl_create_task(self, task_id: str, task: Optional[Union[SQLTask, List[SQLTask]]], pipeline: AbstractPipeline[SQLDag, SQLTask, List[SQLTask], StarlakeDataset]) -> Optional[Union[AbstractTask[SQLTask], AbstractTaskGroup[List[SQLTask]]]]:
        if task is None:
            return None

        elif isinstance(task, list):
            task_group = SQLTaskGroup(
                group_id = task[0].name.split('.')[-1],
                group = task, 
                dag = pipeline.dag,
            )

            with task_group:

                tasks = task
                # sorted_tasks = []

                visited = {}

                def visit(t: Union[SQLTask, List[SQLTask]]) -> Optional[Union[AbstractTask[SQLTask], AbstractTaskGroup[List[SQLTask]]]]:
                    if isinstance(t, List[SQLTask]):
                        v_task_id = t[0].name.split('.')[-1]
                    else:
                        v_task_id = t.name
                    if v_task_id in visited.keys():
                        return visited.get(v_task_id)
                    v = self.sl_create_task(v_task_id.split('.')[-1], t, pipeline)
                    visited.update({v_task_id: v})
                    if isinstance(t, SQLTask):
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
            task._dag = pipeline.dag
            return AbstractTask(task_id, task)

    def sl_create_task_group(self, group_id: str, pipeline: AbstractPipeline[SQLDag, SQLTask, List[SQLTask], StarlakeDataset], **kwargs) -> AbstractTaskGroup[List[SQLTask]]:
        return SQLTaskGroup(
            group_id, 
            group=[],
            dag=pipeline.dag, 
            **kwargs
        )

    @classmethod
    def from_native(cls, native: Any) -> Optional[Union[AbstractTask[SQLTask], AbstractTaskGroup[List[SQLTask]]]]:
        """Create a task or task group from a native object.
        Args:
            native (Any): the native object.
        Returns:
            Optional[Union[AbstractTask[SQLTask], AbstractTaskGroup[List[SQLTask]]]]: the task or task group.
        """
        if isinstance(native, list):
            return SQLTaskGroup(native[0].name.split('.')[-1], native)
        elif isinstance(native, SQLTask):
            return AbstractTask(native.task_id, native)
        else:
            return None
