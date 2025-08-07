from __future__ import annotations

from ai.starlake.common import sort_crons_by_frequency
from ai.starlake.job import StarlakeOrchestrator, StarlakeExecutionMode

from ai.starlake.dataset import StarlakeDataset
from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask

from ai.starlake.snowflake.starlake_snowflake_job import StarlakeSnowflakeJob 
from ai.starlake.snowflake.exceptions import StarlakeSnowflakeError

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task import Cron, StoredProcedureCall, Task
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, DAGRun, _dag_context_stack
from snowflake.snowpark import Row, Session

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

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
        scheduled_datasets: dict = None,
    ) -> None:
        import logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        condition = None

        changes = dict() # tracks the datasets whose changes have to be checked

        streams = set() # set of streams which underlying datasets are scheduled

        not_scheduled_streams = set() # set of streams which underlying datasets are not scheduled

        if not schedule: # if the DAG is not scheduled we will rely on streams to trigger the underlying dag and check if the scheduled datasets without streams have data using CHANGES

            if least_frequent_datasets:
                self.logger.info(f"least frequent datasets: {','.join(list(map(lambda x: x.sink, least_frequent_datasets)))}")
                for dataset in least_frequent_datasets:
                    changes.update({dataset.sink: dataset.cron})

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
            check_freshness = config.get("check_freshness", False)
            logical_date: Optional[Union[str, datetime]] = config.get("logical_date", None)
            if not logical_date:
                query = f"select to_timestamp(system$task_runtime_info('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP'))"
                rows = execute_sql(session, query, "Getting the original scheduled timestamp of the initial graph run in the current group", dry_run)
                if rows:
                    logical_date = rows[0][0]
                else:
                    logical_date = None
            if logical_date:
                if isinstance(logical_date, str):
                    from dateutil import parser
                    start_time = parser.parse(logical_date)
                else:
                    start_time = logical_date
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
                    __error = None
                    try:
                        rows = execute_sql(session, change, f"Checking freshness of {dataset} dataset from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)} using time travel", dry_run)
                        if rows:
                            count = rows[0][0]
                        else:
                            count = 0
                    except Exception as e:
                        __error = e
                        count = 0
                    if count == 0 and check_freshness:
                        raise ValueError(f"Error checking {dataset} dataset freshness from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}{' -> ' + str(__error) if __error else ''}")
                    elif dry_run:
                        print(f"-- Dataset {dataset} has {count} changes from {sl_start_date.strftime(format)} to {sl_end_date.strftime(format)}")
                except CroniterBadCronError:
                    raise ValueError(f"Invalid cron expression: {cron_expr}")
                except Exception as e:
                    if dry_run:
                        print(f"-- {str(e)}")
                    else:
                        raise e

        definition = StoredProcedureCall(
            func = fun, 
            args=[False],
            stage_location=stage_location,
            packages=packages
        )

        if not schedule and not condition:
            if computed_cron:
                schedule = computed_cron
            elif changes:
                sorted_crons_by_frequency: Tuple[Dict[int, List[str]], List[str]] = sort_crons_by_frequency(set(scheduled_datasets.values()))
                most_frequent_cron = sorted_crons_by_frequency[1][0]
                from datetime import datetime
                from croniter import croniter
                iter_cron = croniter(most_frequent_cron, start_time=datetime.now())
                next_run_1 = iter_cron.get_next(datetime)
                next_run_2 = iter_cron.get_next(datetime)
                schedule = next_run_2 - next_run_1
            else: 
                raise StarlakeSnowflakeError("A DAG must be scheduled or depends on stream(s) or at least one dataset with a cron expression")

        super().__init__(
            name=name, 
            schedule=schedule, 
            warehouse=warehouse,
            user_task_managed_initial_warehouse_size=user_task_managed_initial_warehouse_size,
            error_integration=error_integration, 
            comment=comment, 
            task_auto_retry_attempts=task_auto_retry_attempts, 
            allow_overlapping_execution=allow_overlapping_execution,
            user_task_timeout_ms=user_task_timeout_ms,
            suspend_task_after_num_failures=suspend_task_after_num_failures, 
            config=config, 
            session_parameters=session_parameters, 
            stage_location=stage_location, 
            imports=imports, packages=packages, 
            use_func_return_value=use_func_return_value
        )

        self._definition = definition
        self._condition = condition
        self._changes = changes
        self._streams = streams
        self._not_scheduled_streams = not_scheduled_streams

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

    @property
    def definition(self) -> StoredProcedureCall:
        return self._definition

    @property
    def condition(self) -> Optional[str]:
        return self._condition

    @property
    def changes(self) -> dict[str, str]:
        return self._changes

    @property
    def streams(self) -> set[str]:
        return self._streams

    @property
    def not_scheduled_streams(self) -> set[str]:
        return self._not_scheduled_streams

    def has_changes(self) -> bool:
        """Check if the DAG has changes.
        Returns:
            bool: True if the DAG has changes, False otherwise.
        """
        return self._changes.__len__() > 0

    def has_streams(self) -> bool:
        """Check if the DAG has streams.
        Returns:
            bool: True if the DAG has streams, False otherwise.
        """
        return self._streams.__len__() > 0 or self._not_scheduled_streams.__len__() > 0

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
        if self.cron is not None:
            snowflake_schedule = Cron(self.cron, job.timezone)

        computed_cron: Union[Cron, None] = None
        if self.computed_cron_expr is not None:
            computed_cron = Cron(self.computed_cron_expr, job.timezone)

        self._stage_location = job.stage_location
        self._warehouse = job.warehouse

        config = {
            "check_freshness": job.check_freshness,
        }

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
            allow_overlapping_execution=job.allow_overlapping_execution,
            config=config,
            scheduled_datasets = self.scheduled_datasets,
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
            raise StarlakeSnowflakeError("Database and schema must be provided to deploy the pipeline")
        stage_name = f"{database}.{schema}.{self.stage_location}".upper()
        session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name}").collect()
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
            raise StarlakeSnowflakeError("Database and schema must be provided to delete the pipeline")
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
            config = dict() # TODO load the config from the task
            config.update({"check_freshness": False}) # disable freshness check
            if logical_date:
                config.update({"logical_date": logical_date})
            task.suspend()
            import json
            session.sql(f"ALTER TASK IF EXISTS {self.pipeline_id} SET CONFIG = '{json.dumps(config)}'").collect()
            task.resume()
            task.execute()
            from datetime import datetime
            dag_runs = op.get_current_dag_runs(self.dag)
            start = datetime.now()
            def check_started(dag_runs: List[DAGRun]) -> bool:
                import time
                if not dag_runs:
                    raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed to run")
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
                            raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed")
                    elif datetime.now() - start > timedelta(seconds=int(timeout)):
                        raise TimeoutError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} timed out")
                    else:
                        print(f"No task history found for {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} yet")
                        time.sleep(5)
                        return check_status(result)
            status = check_status(SnowflakeDagResult(tasks = list(map(lambda task: SnowflakeTaskResult(name = task.full_name), self.dag.tasks))))
            if status.has_failed:
                raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed -> {status}")
            elif status.is_succeeded:
                print(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} succeeded -> {status}")
            else:
                raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed -> {status}")

        elif mode == StarlakeExecutionMode.BACKFILL:
            if not logical_date:
                raise StarlakeSnowflakeError("Logical date must be provided to backfill the pipeline")
            self.run(logical_date=logical_date, timeout=timeout, mode=StarlakeExecutionMode.RUN, **kwargs)

        else:
            raise StarlakeSnowflakeError(f"Execution mode {mode} is not supported")

    def backfill(self, timeout: str = '120', start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs) -> None:
        """Backfill the pipeline.
        Args:
            timeout (str): the timeout in seconds.
            start_date (Optional[str]): the start date.
            end_date (Optional[str]): the end date.
        """
        # check if backfill has been enabled for the pipeline and that there is no use of streams
        if self.job.allow_overlapping_execution and not self.dag.has_streams():
            from datetime import datetime
            import pytz
            start_time = datetime.fromisoformat(start_date).astimezone(pytz.timezone('UTC'))
            end_time = datetime.fromisoformat(end_date).astimezone(pytz.timezone('UTC'))
            if start_time > end_time:
                raise ValueError("The start date must be before the end date")
            cron = None
            interval = kwargs.get('interval', None)
            schedule = self.dag.schedule
            if schedule:
                if isinstance(schedule, Cron):
                    cron = schedule.expr
                elif isinstance(schedule, timedelta) and not interval:
                    interval = int(schedule.total_seconds() / 60)
                else:
                    raise ValueError("The schedule must be a Cron or timedelta object")
            if interval is None:
                if cron and cron.strip().lower() != 'none':
                    from croniter import croniter
                    # reference datetime
                    base_time = datetime.now()

                    # Init croniter
                    iter = croniter(cron, base_time)

                    # Get the next cron time
                    next_time = iter.get_next(datetime)
                    next_next_time = iter.get_next(datetime)

                    # Calculate the interval in minutes
                    interval = int((next_next_time - next_time).total_seconds() / 60)
            else:
                interval = int(interval)

            session = self.__class__.session(**kwargs)
            import json
            # call the backfill stored procedure
            rows = session.sql(f"call system$task_backfill(?, ?, ?, ?)", (self.pipeline_id, ('TIMESTAMP_LTZ', start_time), ('TIMESTAMP_LTZ', end_time), f'{interval} minutes')).collect()

            start = datetime.now()

            # get the backfill job id and partition count
            if not rows:
                raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} backfill failed")
            else:
                result: dict = json.loads(rows[0][0])

            backfill_job_id: Optional[str] = result.get('backfill_job_id', None)

            partition_count: Optional[int] = result.get('partition_count', None)

            if backfill_job_id is None:
                raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} backfill failed")

            format = '%Y-%m-%d %H:%M:%S%z'
            print(f" Pipeline {self.pipeline_id} is executing backfill job '{backfill_job_id}' with {partition_count} partition(s) from '{start_time.strftime(format)}' to '{end_time.strftime(format)}' using {interval} minutes interval")

            # check the backfill job status
            def check_status():
                import time
                while True:
                    rows = session.sql(f"select * FROM TABLE(information_schema.task_backfill_jobs(root_task_name=>'{self.pipeline_id}')) where BACKFILL_JOB_ID='{backfill_job_id}'").collect()
                    if rows:
                        result: dict = rows[0].as_dict()
                        TOTAL_PARTITIONS_COUNT: int = result.get('TOTAL_PARTITIONS_COUNT', partition_count)
                        EXECUTING_PARTITIONS_COUNT: int = result.get('EXECUTING_PARTITIONS_COUNT', 0)
                        SKIPPED_PARTITIONS_COUNT: int = result.get('SKIPPED_PARTITIONS_COUNT', 0)
                        CANCELED_PARTITIONS_COUNT: int = result.get('CANCELED_PARTITIONS_COUNT', 0)
                        FAILED_PARTITIONS_COUNT: int = result.get('FAILED_PARTITIONS_COUNT', 0)
                        SUCCEEDED_PARTITIONS_COUNT: int = result.get('SUCCEEDED_PARTITIONS_COUNT', 0)
                        if EXECUTING_PARTITIONS_COUNT > 0:
                            print(f"Pipeline {self.pipeline_id} backfill job '{backfill_job_id}' is executing {EXECUTING_PARTITIONS_COUNT} partition(s)")
                            time.sleep(5)
                            return check_status()
                        elif SKIPPED_PARTITIONS_COUNT > 0 or CANCELED_PARTITIONS_COUNT > 0 or FAILED_PARTITIONS_COUNT > 0:
                            raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} backfill job '{backfill_job_id}' failed with {SKIPPED_PARTITIONS_COUNT} skipped partition(s), {CANCELED_PARTITIONS_COUNT} canceled partition(s) and {FAILED_PARTITIONS_COUNT} failed partition(s)")
                        elif SUCCEEDED_PARTITIONS_COUNT == TOTAL_PARTITIONS_COUNT:
                            print(f"Pipeline {self.pipeline_id} backfill job '{backfill_job_id}' succeeded with {SUCCEEDED_PARTITIONS_COUNT} partition(s) succeeded")
                            return
                    elif datetime.now() - start > timedelta(seconds=int(timeout)):
                        raise TimeoutError(f"Pipeline {self.pipeline_id} backfill timed out")
                    else:
                        print(f"No task backfill jobs found for {self.pipeline_id} yet")
                        time.sleep(5)
                        return check_status()

            check_status()

        else:
            super().backfill(timeout=timeout, start_date=start_date, end_date=end_date, **kwargs)

    def get_dag_operation(self, session: Session, database: str, schema: str) -> DAGOperation:
        session.sql(f"CREATE DATABASE IF NOT EXISTS {database}").collect()
        session.sql(f"USE DATABASE {database}").collect()
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}").collect()
        session.sql(f"USE SCHEMA {schema}").collect()
        session.sql(f"CREATE WAREHOUSE IF NOT EXISTS {self.warehouse.upper()}").collect()
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
    def __init__(self, group_id: str, group: List[DAGTask], **kwargs) -> None:
        super().__init__(group_id=group_id, orchestration_cls=SnowflakeOrchestration, group=group, **kwargs)

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
