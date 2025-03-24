from __future__ import annotations

from ai.starlake.job import StarlakeOrchestrator, StarlakeExecutionMode

from ai.starlake.dataset import StarlakeDataset
from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask, AbstractDependency

from ai.starlake.snowflake.starlake_snowflake_job import StarlakeSnowflakeJob

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task import Cron, StoredProcedureCall, Task
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, DAGRun, _dag_context_stack
from snowflake.snowpark import Row, Session

from typing import Any, Callable, List, Optional, Union

from types import ModuleType

from datetime import timedelta

class SnowflakeDag(DAG):
    def __init__(
        self,
        name: str,
        *,
        schedule: Optional[Union[Cron, timedelta]] = None,
        warehouse: Optional[str] = None,
        user_task_managed_initial_warehouse_size: Optional[str] = None,
        error_integration: Optional[str] = None,
        comment: Optional[str] = None,
        task_auto_retry_attempts: Optional[int] = None,
        allow_overlapping_execution: Optional[bool] = None,
        user_task_timeout_ms: Optional[int] = None,
        suspend_task_after_num_failures: Optional[int] = None,
        config: Optional[dict[str, Any]] = None,
        session_parameters: Optional[dict[str, Any]] = None,
        stage_location: Optional[str] = None,
        imports: Optional[list[Union[str, tuple[str, str]]]] = None,
        packages: Optional[list[Union[str, ModuleType]]] = None,
        use_func_return_value: bool = False,
        computed_cron: Optional[Cron] = None,
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
                        not_scheduled_streams.add(f"SYSTEM$STREAM_HAS_DATA('{dataset.stream}')")
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
                        streams.add(f"SYSTEM$STREAM_HAS_DATA('{dataset.stream}')")
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

        def execute_sql(session: Session, query: Optional[str], message: Optional[str] = None, dry_run: bool = False) -> List[Row]:
            """Execute the SQL.
            Args:
                session (Session): The Snowflake session.
                query (str): The SQL query to execute.
                message (Optional[str], optional): The optional message. Defaults to None.
                mode (Optional[StarlakeExecutionMode], optional): The optional execution mode. Defaults to None.
            Returns:
                List[Row]: The rows.
            """
            if query:
                if dry_run and message:
                    print(f"-- {message}")
                if dry_run:
                    print(f"{query};")
                    return []
                else:
                    return session.sql(query).collect()
            else:
                return []

        format = '%Y-%m-%d %H:%M:%S%z'

        def fun(session: Session, dry_run: bool) -> None:
            from croniter import croniter
            from croniter.croniter import CroniterBadCronError
            from datetime import datetime

            # get the original scheduled timestamp of the initial graph run in the current group
            # For graphs that are retried, the returned value is the original scheduled timestamp of the initial graph run in the current group.
            if not dry_run:
                config = session.call("system$get_task_graph_config")
            else:
                config = None
                print("-- SL_START")
            if config:
                import json
                config = json.loads(config)
            else:
                config = {}
            original_schedule = config.get("logical_date", None)
            if not original_schedule:
                query = f"select to_timestamp(system$task_runtime_info('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP'))"
                rows = execute_sql(session, query, "Getting the original scheduled timestamp of the initial graph run in the current group", dry_run)
                if rows:
                    original_schedule = rows[0][0]
                else:
                    original_schedule = None
            if original_schedule:
                if isinstance(original_schedule, str):
                    from dateutil import parser
                    start_time = parser.parse(original_schedule)
                else:
                    start_time = original_schedule
            else:
                start_time = datetime.fromtimestamp(datetime.now().timestamp())

            def check_if_dataset_exists(dataset: str) -> bool:
                query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{dataset}'"
                rows = execute_sql(session, query, f"Checking if dataset {dataset} exists", dry_run)
                if dry_run:
                    return True
                else:
                    return rows.__len__() > 0

            for dataset, cron_expr in changes.items():
                if not check_if_dataset_exists(dataset):
                    raise ValueError(f"Dataset {dataset} does not exist")
                try:
                    # enabling change tracking for the dataset - should be done once and when we create our datasets
                    query = f"ALTER TABLE {dataset} SET CHANGE_TRACKING = TRUE"
                    execute_sql(session, query, f"Enabling change tracking for dataset {dataset}", dry_run)
                    croniter(cron_expr)
                    iter = croniter(cron_expr, start_time)
                    # get the start and end date of the current cron iteration
                    curr = iter.get_current(datetime)
                    previous = iter.get_prev(datetime)
                    next = croniter(cron_expr, previous).get_next(datetime)
                    if curr == next :
                        sl_end_date = curr
                    else:
                        sl_end_date = previous
                    sl_start_date = croniter(cron_expr, sl_end_date).get_prev(datetime)
                    change = f"SELECT count(*) FROM {dataset} CHANGES(INFORMATION => DEFAULT) AT(TIMESTAMP => '{sl_start_date.strftime(format)}') END (TIMESTAMP => '{sl_end_date.strftime(format)}')"
                    rows = execute_sql(session, change, f"Checking changes for dataset {dataset} from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}", dry_run)
                    if rows:
                        count = rows[0][0]
                    else:
                        count = 0
                    if count == 0:
                        error=f"Dataset {dataset} has no changes from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}"
                        print(error)
                        if not dry_run:
                            raise ValueError(error)
                    print(f"Dataset {dataset} has data from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}")
                except CroniterBadCronError:
                    raise ValueError(f"Invalid cron expression: {cron_expr}")
                except Exception as e:
                    raise ValueError(f"Error checking changes for dataset {dataset}: {str(e)}")

        definition = StoredProcedureCall(
            func = fun, 
            args=[False],
            stage_location=stage_location,
            packages=packages
        )

        if not schedule and not condition:
            if computed_cron:
                schedule = computed_cron
            else:
                raise ValueError("A DAG must be scheduled or have a condition")

        super().__init__(name, schedule=schedule, warehouse=warehouse, user_task_managed_initial_warehouse_size=user_task_managed_initial_warehouse_size, error_integration=error_integration, comment=comment, task_auto_retry_attempts=task_auto_retry_attempts, allow_overlapping_execution=allow_overlapping_execution, user_task_timeout_ms=user_task_timeout_ms, suspend_task_after_num_failures=suspend_task_after_num_failures, config=config, session_parameters=session_parameters, stage_location=stage_location, imports=imports, packages=packages, use_func_return_value=use_func_return_value)
        self.definition = definition
        self.condition = condition

    def _to_low_level_task(self) -> Task:
        return Task(
            name=f"{self.name}",
            definition=self.definition,
            condition=self.condition,
            schedule=self.schedule,
            warehouse=self.warehouse,
            # user_task_managed_initial_warehouse_size=self.user_task_managed_initial_warehouse_size,
            error_integration=self.error_integration,
            comment=self.comment,
            task_auto_retry_attempts=self.task_auto_retry_attempts,
            allow_overlapping_execution=self.allow_overlapping_execution,
            user_task_timeout_ms=self.user_task_timeout_ms,
            suspend_task_after_num_failures=self.suspend_task_after_num_failures,
            session_parameters=self.session_parameters,
            config=self.config,
        )

class SnowflakePipeline(AbstractPipeline[SnowflakeDag, DAGTask, List[DAGTask], StarlakeDataset], StarlakeDataset):
    def __init__(self, job: StarlakeSnowflakeJob, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, orchestration: Optional[AbstractOrchestration[SnowflakeDag, DAGTask, List[DAGTask], StarlakeDataset]] = None, **kwargs) -> None:
        def fun(upstream: Union[DAGTask, List[DAGTask]], downstream: Union[DAGTask, List[DAGTask]]) -> None:
            if isinstance(upstream, DAGTask):
                upstream.add_successors(downstream)
            elif isinstance(upstream, list):
                for task in upstream:
                    task.add_successors(downstream)

        super().__init__(job, orchestration_cls=SnowflakeOrchestration, dag=None, schedule=schedule, dependencies=dependencies, orchestration=orchestration, add_dag_dependency=fun, **kwargs)

        snowflake_schedule: Union[Cron, None] = None
        computed_cron: Union[Cron, None] = None
        if self.cron is not None:
            snowflake_schedule = Cron(self.cron, job.timezone)
        elif self.datasets is not None:
            if self.computed_cron_expr is not None:
                computed_cron = Cron(self.computed_cron_expr, job.timezone)
            else:
                snowflake_schedule = timedelta(minutes=60) # FIXME should we keep a default value here?

        self._stage_location = job.stage_location
        self._warehouse = job.warehouse

        self.dag = SnowflakeDag(
            name=self.pipeline_id,
            schedule=snowflake_schedule,
            warehouse=self.warehouse,
            comment=job.caller_globals.get('description', f"Pipeline {self.pipeline_id}"),
            stage_location=self.stage_location,
            packages=job.packages,
            computed_cron=computed_cron,
            not_scheduled_datasets=self.not_scheduled_datasets,
            least_frequent_datasets=self.least_frequent_datasets,
            most_frequent_datasets=self.most_frequent_datasets,
            task_auto_retry_attempts=job.retries,
            
        )

    def __enter__(self):
        _dag_context_stack.append(self.dag)
        return super().__enter__()
    
    def __exit__(self, exc_type, exc_value, traceback):
        _dag_context_stack.pop()

        return super().__exit__(exc_type, exc_value, traceback)

    @property
    def stage_location(self) -> Optional[str]:
        return self._stage_location

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    @classmethod
    def session(cls, **kwargs) -> Session:
        import os
        env = os.environ.copy() # Copy the current environment variables
        options = {
            "account": kwargs.get('SNOWFLAKE_ACCOUNT', env.get('SNOWFLAKE_ACCOUNT', None)),
            "user": kwargs.get('SNOWFLAKE_USER', env.get('SNOWFLAKE_USER', None)),
            "password": kwargs.get('SNOWFLAKE_PASSWORD', env.get('SNOWFLAKE_PASSWORD', None)),
            "database": kwargs.get('SNOWFLAKE_DB', env.get('SNOWFLAKE_DB', None)),
            "schema": kwargs.get('SNOWFLAKE_SCHEMA', env.get('SNOWFLAKE_SCHEMA', None)),
            "warehouse": kwargs.get('SNOWFLAKE_WAREHOUSE', env.get('SNOWFLAKE_WAREHOUSE', None)),
        }
        return Session.builder.configs(options).create()

    def deploy(self, **kwargs) -> None:
        """Deploy the pipeline."""
        import os
        env = os.environ.copy() # Copy the current environment variables
        session = self.__class__.session(**kwargs)
        database = kwargs.get('SNOWFLAKE_DB', env.get('SNOWFLAKE_DB', None))
        schema = kwargs.get('SNOWFLAKE_SCHEMA', env.get('SNOWFLAKE_SCHEMA', None))
        if database is None or schema is None:
            raise ValueError("Database and schema must be provided to deploy the pipeline")
        stage_name = f"{database}.{schema}.{self.stage_location}".upper()
        result = session.sql(f"SHOW STAGES LIKE '{stage_name.split('.')[-1]}'").collect()
        if not result:
            session.sql(f"CREATE STAGE {stage_name}").collect()
        session.custom_package_usage_config = {"enabled": True, "force_push": True}
        op = self.get_dag_operation(session, database, schema)
        # op.delete(pipeline_id)
        op.deploy(self.dag, mode = CreateMode.or_replace)
        print(f"Pipeline {self.pipeline_id} deployed")

    def delete(self, **kwargs) -> None:
        import os
        env = os.environ.copy() # Copy the current environment variables
        session = self.__class__.session(**kwargs)
        database = kwargs.get('SNOWFLAKE_DB', env.get('SNOWFLAKE_DB', None))
        schema = kwargs.get('SNOWFLAKE_SCHEMA', env.get('SNOWFLAKE_SCHEMA', None))
        if database is None or schema is None:
            raise ValueError("Database and schema must be provided to delete the pipeline")
        op = self.get_dag_operation(session, database, schema)
        op.delete(self.pipeline_id)
        print(f"Pipeline {self.pipeline_id} deleted")

    def run(self, logical_date: Optional[str] = None, timeout: str = '120', mode: StarlakeExecutionMode = StarlakeExecutionMode.RUN, **kwargs) -> None:
        """Run the pipeline.
        Args:
            logical_date (Optional[str]): the logical date.
            timeout (str): the timeout in seconds.
            mode (StarlakeExecutionMode): the execution mode.
        """
        session = self.__class__.session(**kwargs)
        if mode == StarlakeExecutionMode.DRY_RUN:
            def dry_run(definition) -> None:
                if isinstance(definition, StoredProcedureCall):
                    func = definition.func
                    if isinstance(func, Callable):
                        func.__call__(session = session, dry_run = True)
            dag = self.dag
            dry_run(dag.definition)
            tasks = dag.tasks
            for task in tasks:
                definition = task.definition
                dry_run(definition)

        elif mode == StarlakeExecutionMode.RUN:
            import os
            env = os.environ.copy() # Copy the current environment variables
            database = kwargs.get('SNOWFLAKE_DB', env.get('SNOWFLAKE_DB', None))
            schema = kwargs.get('SNOWFLAKE_SCHEMA', env.get('SNOWFLAKE_SCHEMA', None))
            op = self.get_dag_operation(session, database, schema)
            task = op.schema.tasks[self.pipeline_id]
            if logical_date:
                import json
                config = dict()
                config.update({"logical_date": logical_date})
                task.suspend()
                session.sql(f"ALTER TASK IF EXISTS {self.pipeline_id} SET CONFIG = '{json.dumps(config)}'").collect()
                task.resume()
            task.execute()
            from datetime import datetime
            dag_runs = op.get_current_dag_runs(self.dag)
            start = datetime.now()
            def check_started(dag_runs: List[DAGRun]) -> bool:
                import time
                if not dag_runs:
                    raise ValueError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed to run")
                else:
                    while True:
                        dag_runs_sorted: List[DAGRun] = sorted(dag_runs, key=lambda run: run.scheduled_time, reverse=True)
                        last_run: DAGRun = dag_runs_sorted[0]
                        state = last_run.state
                        print(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} is {state.lower()}")
                        if state.upper() == 'EXECUTING':
                            return True
                        else:
                            if datetime.now() - start > timedelta(seconds=int(timeout)):
                                raise TimeoutError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} timed out")
                            time.sleep(5)
                            return check_started(op.get_current_dag_runs(self.dag))
            check_started(dag_runs)
            def check_status(result: SnowflakeDagResult) -> SnowflakeDagResult:
                import time
                while True:
                    rows: List[Row] = session.sql(
                        f"""SELECT NAME, STATE, GRAPH_RUN_GROUP_ID 
                            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
                            WHERE GRAPH_RUN_GROUP_ID IN (
                                SELECT GRAPH_RUN_GROUP_ID FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
                            WHERE NAME ilike '{self.pipeline_id}%' AND COMPLETED_TIME is not null
                                ORDER BY COMPLETED_TIME DESC
                                LIMIT 1
                            )""").collect()
                    if rows:
                        for row in rows:
                            d = row.as_dict()
                            name = d.get('NAME', None)
                            state = d.get('STATE', None)
                            graph_run_group_id = d.get('GRAPH_RUN_GROUP_ID', None)
                            if name and state and graph_run_group_id:
                                result.update_task_result(name, state, graph_run_group_id)
                        if result.has_failed:
                            return result
                        elif result.is_succeeded:
                            return result
                        elif datetime.now() - start > timedelta(seconds=int(timeout)):
                            raise TimeoutError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} timed out")
                        elif result.is_executing:
                            print(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} is still executing")
                            time.sleep(5)
                            return check_status(result)
                        else:
                            # if the pipeline is not executing and has not failed, we consider it as failed
                            raise ValueError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed")
                    elif datetime.now() - start > timedelta(seconds=int(timeout)):
                        raise TimeoutError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} timed out")
                    else:
                        print(f"No task history found for {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} yet")
                        time.sleep(5)
                        return check_status(result)
            status = check_status(SnowflakeDagResult(tasks = list(map(lambda task: SnowflakeTaskResult(name = task.full_name), self.dag.tasks))))
            if status.has_failed:
                raise ValueError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed -> {status}")
            elif status.is_succeeded:
                print(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} succeeded -> {status}")
            else:
                raise ValueError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed -> {status}")

        elif mode == StarlakeExecutionMode.BACKFILL:
            if not logical_date:
                raise ValueError("Logical date must be provided to backfill the pipeline")
            self.run(logical_date=logical_date, timeout=timeout, mode=StarlakeExecutionMode.RUN, **kwargs)

        else:
            raise ValueError(f"Execution mode {mode} is not supported")

    def get_dag_operation(self, session: Session, database: str, schema: str) -> DAGOperation:
        session.sql(f"USE DATABASE {database}").collect()
        session.sql(f"USE SCHEMA {schema}").collect()
        session.sql(f"USE WAREHOUSE {self.warehouse.upper()}").collect()
        root = Root(session)
        schema = root.databases[database].schemas[schema]
        return DAGOperation(schema)

class SnowflakeTaskResult:
    def __init__(self, name: str, state: str = "EXECUTING", graph_run_group_id: Optional[str] = None) -> None:
        self.name = name
        self.state = state
        self.graph_run_group_id = graph_run_group_id

    @property
    def is_executing(self) -> bool:
        return self.state == "EXECUTING"

    @property
    def has_failed(self) -> bool:
        return self.state == 'FAILED'

    @property
    def is_succeeded(self) -> bool:
        return self.state == 'SUCCEEDED'

    @property
    def state_as_str(self) -> str:
        if self.is_executing:
            return "is executing"
        return self.state.lower()

    def __repr__(self) -> str:
        return f"Task {self.name} within graph_run_group_id {self.graph_run_group_id} {(self.state_as_str)}"

    def __str__(self) -> str:
        return self.__repr__()

class SnowflakeDagResult:
    def __init__(self, tasks: List[SnowflakeTaskResult]) -> None:
        self.tasks_map = {task.name.lower(): task for task in tasks}

    @property
    def tasks(self) -> List[SnowflakeTaskResult]:
        return list(self.tasks_map.values())

    @property
    def has_failed(self) -> bool:
        return any([task.has_failed for task in self.tasks])

    @property
    def is_executing(self) -> bool:
        return any([task.is_executing for task in self.tasks])

    @property
    def is_succeeded(self) -> bool:
        return all([task.is_succeeded for task in self.tasks])

    def update_task_result(self, name: str, state: str, graph_run_group_id: str) -> SnowflakeTaskResult:
        task = self.tasks_map.get(name.lower(), None)
        if task:
            task.state = state
            task.graph_run_group_id = graph_run_group_id
        else:
            task = SnowflakeTaskResult(name, state, graph_run_group_id)
        self.tasks_map.update({name.lower(): task})
        return task

    def __repr__(self) -> str:
        return f"Tasks {self.tasks}"

    def __str__(self) -> str:
        return self.__repr__()

class SnowflakeTaskGroup(AbstractTaskGroup[List[DAGTask]]):
    def __init__(self, group_id: str, group: List[DAGTask], dag: Optional[SnowflakeDag] = None, **kwargs) -> None:
        super().__init__(group_id=group_id, orchestration_cls=SnowflakeOrchestration, group=group, **kwargs)
        self.dag = dag
        self._group_as_map = dict()

    def __exit__(self, exc_type, exc_value, traceback):
        for dep in self.dependencies:
            if isinstance(dep, SnowflakeTaskGroup):
                self._group_as_map.update({dep.id: dep})
            elif isinstance(dep, AbstractTask):
                self._group_as_map.update({dep.id: dep.task})
        return super().__exit__(exc_type, exc_value, traceback)

    @property
    def group_leaves(self) -> List[DAGTask]:
        leaves = []
        for leaf in self.leaves_keys:
            dep = self._group_as_map.get(leaf, None)
            if dep:
                if isinstance(dep, SnowflakeTaskGroup):
                    leaves.extend(dep.group_leaves)
                else:
                    leaves.append(dep)
        return leaves

    @property
    def group_roots(self) -> List[DAGTask]:
        roots = []
        for root in self.roots_keys:
            dep = self._group_as_map.get(root, None)
            if dep:
                if isinstance(dep, SnowflakeTaskGroup):
                    roots.extend(dep.group_roots)
                else:
                    roots.append(dep)
        return roots

class SnowflakeOrchestration(AbstractOrchestration[SnowflakeDag, DAGTask, List[DAGTask], StarlakeDataset]):
    def __init__(self, job: StarlakeSnowflakeJob, **kwargs) -> None:
        """Overrides AbstractOrchestration.__init__()
        Args:
            job (StarlakeSnowflakeJob): The Snowflake job that will generate the tasks within the pipeline.
        """
        super().__init__(job, **kwargs) 

    @classmethod
    def sl_orchestrator(cls) -> str:
        return StarlakeOrchestrator.SNOWFLAKE

    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[SnowflakeDag, DAGTask, List[DAGTask], StarlakeDataset]:
        """Create the Starlake pipeline to orchestrate.

        Args:
            schedule (Optional[StarlakeSchedule]): The optional schedule
            dependencies (Optional[StarlakeDependencies]): The optional dependencies
        
        Returns:
            AbstractPipeline[SnowflakeDag, DAGTask, List[DAGTask], StarlakeDataset]: The pipeline to orchestrate.
        """
        return SnowflakePipeline(
            self.job, 
            schedule, 
            dependencies, 
            self
        )

    def sl_create_task(self, task_id: str, task: Optional[Union[DAGTask, List[DAGTask]]], pipeline: AbstractPipeline[SnowflakeDag, DAGTask, List[DAGTask], StarlakeDataset]) -> Optional[Union[AbstractTask[DAGTask], AbstractTaskGroup[List[DAGTask]]]]:
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
            task._dag = pipeline.dag
            return AbstractTask(task_id, task)

    def sl_create_task_group(self, group_id: str, pipeline: AbstractPipeline[SnowflakeDag, DAGTask, List[DAGTask], StarlakeDataset], **kwargs) -> AbstractTaskGroup[List[DAGTask]]:
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
