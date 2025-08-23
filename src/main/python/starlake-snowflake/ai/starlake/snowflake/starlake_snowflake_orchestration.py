from __future__ import annotations

from ai.starlake.common import most_frequent_crons
from ai.starlake.job import StarlakeOrchestrator, StarlakeExecutionMode

from ai.starlake.dataset import StarlakeDataset, StarlakeDatasetType
from ai.starlake.orchestration import AbstractOrchestration, StarlakeSchedule, StarlakeDependencies, AbstractPipeline, AbstractTaskGroup, AbstractTask

from ai.starlake.snowflake.starlake_snowflake_job import StarlakeSnowflakeJob 
from ai.starlake.snowflake.exceptions import StarlakeSnowflakeError

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.task import Cron, StoredProcedureCall, Task
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, DAGRun, _dag_context_stack
from snowflake.snowpark import Row, Session

from typing import Any, Callable, List, Optional, Union

from types import ModuleType

from datetime import datetime, timedelta

from croniter import croniter

import pytz

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
        timezone: str = 'UTC',
        min_timedelta_between_runs: int = 900, # default to 15 minutes
        start_date: datetime = None,
        data_cycle: Optional[str] = None,
        optional_dataset_enabled: bool = False,
        beyond_data_cycle_enabled: bool = False,
        ai_zip: str = None
    ) -> None:
        from ai.starlake.common import is_valid_cron, get_cron_frequency, scheduled_dates_range
        from ai.starlake.helper import datetime_format, SnowflakeDAGHelper


        helper = SnowflakeDAGHelper(name=name, timezone=timezone)

        info = helper.info
        warning = helper.warning
        error = helper.error
        # debug = helper.debug

        execute_sql = helper.execute_sql
        get_execution_date = helper.get_execution_date
        as_datetime = helper.as_datetime
        is_current_graph_scheduled = helper.is_current_graph_scheduled
        get_dag_logical_date = helper.get_dag_logical_date
        get_start_end_dates = helper.get_start_end_dates
        get_previous_dag_run = helper.get_previous_dag_run
        check_if_dataset_exists = helper.check_if_dataset_exists
        find_dataset_event = helper.find_dataset_event

        condition = None

        datasets = dict() # tracks the datasets whose changes have to be checked

        streams = set() # set of streams which underlying datasets are scheduled

        not_scheduled_streams = set() # set of streams which underlying datasets are not scheduled

        most_frequent = set()

        if not schedule and least_frequent_datasets:
            info(f"Least frequent datasets: {','.join(list(map(lambda x: x.sink, least_frequent_datasets)))}", dry_run=False)
            for dataset in least_frequent_datasets:
                datasets.update({dataset.sink: (dataset.cron, dataset.freshness)})

        if not_scheduled_datasets:
            info(f"Not scheduled datasets: {','.join(list(map(lambda x: x.sink, not_scheduled_datasets)))}", dry_run=False)
            for dataset in not_scheduled_datasets:
                if dataset.stream:
                    not_scheduled_streams.add(f"SYSTEM$STREAM_HAS_DATA('{dataset.stream}')")
                elif not schedule:
                    datasets.update({dataset.sink: (None, dataset.freshness)})

        if most_frequent_datasets:
            info(f"Most frequent datasets: {','.join(list(map(lambda x: x.sink, most_frequent_datasets)))}", dry_run=False)
            most_frequent = set(most_frequent_crons(list(map(lambda x: x.cron, most_frequent_datasets))))
            for dataset in most_frequent_datasets:
                if dataset.stream:
                    streams.add(f"SYSTEM$STREAM_HAS_DATA('{dataset.stream}')")
                elif not schedule:
                    datasets.update({dataset.sink: (dataset.cron, dataset.freshness)})

        if streams:
            condition = ' OR '.join(streams)

        if not_scheduled_streams:
            if condition:
                condition = f"({condition}) AND ({' AND '.join(not_scheduled_streams)})"
            else:
                condition = ' AND '.join(not_scheduled_streams)

        computed_cron_expr = None
        if computed_cron:
            computed_cron_expr = computed_cron.expr

        def fun(session: Session, dry_run: bool, logical_date: Optional[str] = None) -> None:
            query = "ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_LTZ'"
            execute_sql(session, query, "Set session timestamp type mapping", False)

            ts = get_execution_date(session, dry_run=dry_run)

            backfill: bool = False
            if allow_overlapping_execution:
                query = "SELECT SYSTEM$TASK_RUNTIME_INFO('IS_BACKFILL')::boolean"
                rows = execute_sql(session, query, "Check if the current running dag is a backfill", dry_run)
                if rows.__len__() == 1:
                    backfill = rows[0][0]

            manual: bool = not is_current_graph_scheduled(session, dry_run)

            if not logical_date:
                logical_date = get_dag_logical_date(session, ts, backfill, dry_run=dry_run)
            logical_date = as_datetime(logical_date)

            if computed_cron_expr and not backfill:
                # if a cron expression has been provided, the scheduled date corresponds to the end date determined by applying the cron expression to the logical date
                (_, scheduled_date) = get_start_end_dates(computed_cron_expr, logical_date)
            else:
                scheduled_date = logical_date

            previous_dag_checked: Optional[datetime] = None
            previous_dag_ts: Optional[datetime] = None

            previous_dag_run = get_previous_dag_run(session, logical_date, False, at_scheduled_date=False)

            if previous_dag_run:
                previous_dag_checked = previous_dag_run[0]
                previous_dag_ts = previous_dag_run[1]
                info(f"Found previous succeeded dag run with scheduled date {previous_dag_checked} and start date {previous_dag_ts}", dry_run=dry_run)

            if not previous_dag_checked:
                # if the dag never run successfuly, 
                # we set the previous dag checked to the start date of the dag
                previous_dag_checked = start_date
                info(f"No previous succeeded dag run found, we set the previous dag checked to the start date of the dag {previous_dag_checked}", dry_run=dry_run)

            last_dag_checked: Optional[datetime] = None
            last_dag_ts: Optional[datetime] = None
            if not backfill:
                last_dag_run = get_previous_dag_run(session, logical_date, False, at_scheduled_date=True)
                if last_dag_run:
                    last_dag_checked = last_dag_run[0]
                    last_dag_ts = last_dag_run[1]
                    info(f"Found last succeeded dag run with scheduled date {last_dag_checked} and start date {last_dag_ts}", dry_run=dry_run)

            skipped = False

            if last_dag_ts and last_dag_checked:
                if computed_cron_expr:
                    # Compute the scheduled date for the last DAG run if a computed cron expression has been provided for the DAG
                    (_, last_dag_checked) = get_start_end_dates(computed_cron_expr, last_dag_checked)
                    info(f"Computed scheduled date for last DAG run: {last_dag_checked}", dry_run=dry_run)
                if not backfill and last_dag_checked.strftime(datetime_format) == scheduled_date.strftime(datetime_format):
                    # if the last DAG run has the same scheduled date as the current one, we check if it was run less than min_timedelta_between_runs seconds ago
                    diff: timedelta = ts - last_dag_ts
                    if not manual:
                        # we run successfuly this dag for the same scheduled date, we should skip the current execution
                        warning(f"The last succeeded dag run has been executed at {last_dag_ts} with the same scheduled date {last_dag_checked}... The current DAG execution will be skipped", dry_run=dry_run)
                        if not dry_run:
                            skipped = True
                    elif diff.total_seconds() <= min_timedelta_between_runs:
                        # we run successfuly this dag for the same scheduled date, we should skip the current execution
                        warning(f"The last succeeded dag run has been executed at {last_dag_ts} with the same scheduled date {last_dag_checked} less than {min_timedelta_between_runs} seconds ago ({diff.seconds} seconds)... The current DAG execution will be skipped", dry_run=dry_run)
                        if not dry_run:
                            skipped = True

            if not skipped:
                missing_datasets = []

                data_cycle_freshness: Optional[timedelta] = None
                if data_cycle and data_cycle.lower() != "none":
                    # the freshness of the data cycle is the time delta between 2 iterations of its schedule
                    data_cycle_freshness = get_cron_frequency(data_cycle)

                for dataset, (original_cron, freshness) in datasets.items():
                    if not check_if_dataset_exists(session, dataset):
                        error(f"Dataset {dataset} does not exist", dry_run=dry_run)
                        if not dry_run:
                            skipped = True
                            break

                    cron = original_cron or data_cycle
                    scheduled = cron and is_valid_cron(cron)
                    optional = False
                    beyond_data_cycle_allowed = False

                    if data_cycle_freshness:
                        original_scheduled = original_cron and is_valid_cron(original_cron)
                        if optional_dataset_enabled:
                            # we check if the dataset is optional by comparing its freshness with that of the data cycle
                            # the freshness of a scheduled dataset is the time delta between 2 iterations of its schedule
                            # the freshness of a non scheduled dataset is defined by its freshness parameter
                            optional = (original_scheduled and abs(data_cycle_freshness.total_seconds()) < abs(get_cron_frequency(original_cron).total_seconds())) or (not original_scheduled and abs(data_cycle_freshness.total_seconds()) < freshness)
                        if beyond_data_cycle_enabled:
                            # we check if the dataset scheduled date is allowed to be beyond the data cycle by comparing its freshness with that of the data cycle
                            beyond_data_cycle_allowed = (original_scheduled and abs(data_cycle_freshness.total_seconds()) < abs(get_cron_frequency(original_cron).total_seconds() + freshness)) or (not original_scheduled and abs(data_cycle_freshness.total_seconds()) < freshness)

                    if optional:
                        info(f"Dataset {dataset.uri} is optional, we skip it", dry_run=dry_run)
                        continue
                    elif scheduled:
                        if not cron in most_frequent or cron.startswith('0 0') or get_cron_frequency(cron).days == 0:
                            dates_range = scheduled_dates_range(cron, scheduled_date)
                        else:
                            dates_range = scheduled_dates_range(cron, croniter(cron, scheduled_date.replace(hour=0, minute=0, second=0, microsecond=0)).get_next(datetime))
                        scheduled_date_to_check_min = dates_range[0]
                        scheduled_date_to_check_max = max(scheduled_date, dates_range[1]) # we check the dataset events around the scheduled date
                        if not original_cron and previous_dag_checked > scheduled_date_to_check_min:
                            scheduled_date_to_check_min = previous_dag_checked
                        if beyond_data_cycle_allowed:
                            scheduled_date_to_check_min = scheduled_date_to_check_min - timedelta(seconds=freshness)
                            scheduled_date_to_check_max = scheduled_date_to_check_max + timedelta(seconds=freshness)
                        if find_dataset_event(session, dataset, scheduled_date_to_check_min, scheduled_date_to_check_max, scheduled_date, False):
                            info(f"Dataset {dataset} has been found in the audit table between {scheduled_date_to_check_min.strftime(datetime_format)} and {scheduled_date_to_check_max.strftime(datetime_format)}", dry_run=dry_run)
                        else:
                            warning(f"Dataset {dataset} has not been found in the audit table between {scheduled_date_to_check_min.strftime(datetime_format)} and {scheduled_date_to_check_max.strftime(datetime_format)}", dry_run=dry_run)
                            missing_datasets.append(dataset)
                    else:
                        # we check if one dataset event at least has been published since the previous dag checked and around the scheduled date +- freshness in seconds - it should be the closest one
                        scheduled_date_to_check_min = previous_dag_checked - timedelta(seconds=freshness)
                        scheduled_date_to_check_max = scheduled_date + timedelta(seconds=freshness)
                        if find_dataset_event(session, dataset, scheduled_date_to_check_min, scheduled_date_to_check_max, scheduled_date, False):
                            info(f"Dataset {dataset} has been found in the audit table between {scheduled_date_to_check_min.strftime(datetime_format)} and {scheduled_date_to_check_max.strftime(datetime_format)}", dry_run=dry_run)
                        else:
                            warning(f"Dataset {dataset} has not been found in the audit table between {scheduled_date_to_check_min.strftime(datetime_format)} and {scheduled_date_to_check_max.strftime(datetime_format)}", dry_run=dry_run)
                            missing_datasets.append(dataset)

                if missing_datasets:
                    warning(f"The following datasets are missing: {', '.join(missing_datasets)}, the current DAG execution will be skipped", dry_run=dry_run)
                    skipped = True
                elif datasets:
                    info(f"All datasets are present: {', '.join(datasets.keys())}, the current DAG will continue its execution", dry_run=dry_run)
                else:
                    info("No datasets to check, the current DAG will continue its execution", dry_run=dry_run)

            if not skipped:
                # if the dag has to be run, we set the return value to the logical date of the running dag
                query = f"SELECT SYSTEM$SET_RETURN_VALUE('{scheduled_date.strftime(datetime_format)}')"
                execute_sql(session, query, f"Set return value to the logical date of the running DAG '{scheduled_date.strftime(datetime_format)}'", dry_run)
            else:
                # if the dag has to be skipped, we set the return value to an empty string
                query = "SELECT SYSTEM$SET_RETURN_VALUE('')"
                execute_sql(session, query, "Set return value to an empty string because the DAG has to be skipped", dry_run)

        definition = StoredProcedureCall(
            func = fun, 
            args=[False, None],
            stage_location=stage_location,
            imports=[(ai_zip, 'ai')],
            packages=packages
        )

        if not schedule:
            # if no schedule is provided, we set the schedule to a timedelta based on the minimum time delta between runs
            schedule = timedelta(seconds=min_timedelta_between_runs)

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
        self._datasets = datasets
        self._streams = streams
        self._not_scheduled_streams = not_scheduled_streams
        self._timezone = timezone

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
    def datasets(self) -> dict[str, str]:
        return self._datasets

    @property
    def streams(self) -> set[str]:
        return self._streams

    @property
    def not_scheduled_streams(self) -> set[str]:
        return self._not_scheduled_streams

    @property
    def timezone(self) -> str:
        return self._timezone

    def has_datasets(self) -> bool:
        """Check if the DAG has datasets.
        Returns:
            bool: True if the DAG has datasets, False otherwise.
        """
        return self._datasets.__len__() > 0

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

        if self.run_dependencies_first and dependencies:
            datasets = dependencies.retrieve_datasets(self.filtered_datasets, job.sl_schedule_parameter_name, job.sl_schedule_format, recursive=True, datasetType=StarlakeDatasetType.LOAD)
            self.set_cron_expr(datasets)

        snowflake_schedule: Union[Cron, None] = None
        if self.cron is not None:
            snowflake_schedule = Cron(self.cron, job.timezone)

        computed_cron: Union[Cron, None] = None
        if self.computed_cron_expr is not None:
            computed_cron = Cron(self.computed_cron_expr, job.timezone)

        self._stage_location = job.stage_location
        self._warehouse = job.warehouse

        config = {}

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
            timezone=job.timezone,
            min_timedelta_between_runs=job.min_timedelta_between_runs,
            start_date=job.start_date,
            data_cycle=job.data_cycle,
            optional_dataset_enabled=job.optional_dataset_enabled,
            beyond_data_cycle_enabled=job.beyond_data_cycle_enabled,
            ai_zip=job.ai_zip,
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
                        func.__call__(session = session, dry_run = True, logical_date = logical_date)
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
            if logical_date:
                config.update({"logical_date": logical_date})
            task.suspend()
            import json
            session.sql(f"ALTER TASK IF EXISTS {self.pipeline_id} SET CONFIG = '{json.dumps(config)}'").collect()
            task.resume()
            task.execute()
            dag_runs = op.get_current_dag_runs(self.dag)
            start = datetime.now()
            def check_started(dag_runs: List[DAGRun]) -> bool:
                if not dag_runs:
                    raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed to run")
                else:
                    while True:
                        import time
                        dag_runs_sorted: List[DAGRun] = sorted(dag_runs, key=lambda run: run.query_start_time, reverse=True)
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
            datetime_format = '%Y-%m-%d %H:%M:%S %z'
            start_as_timestamp = start.astimezone(pytz.timezone(self.dag.timezone)).strftime(datetime_format)
            print(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} started at {start_as_timestamp}")
            def check_status(result: SnowflakeDagResult) -> SnowflakeDagResult:
                import time
                while True:
                    rows: List[Row] = session.sql(
                        f"""SELECT NAME, STATE, GRAPH_RUN_GROUP_ID 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE GRAPH_RUN_GROUP_ID IN (
    SELECT GRAPH_RUN_GROUP_ID FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
    WHERE 
        NAME ilike '{self.pipeline_id}%' 
        AND COMPLETED_TIME is not null
        AND COMPLETED_TIME >= '{start_as_timestamp}'
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
                        if result.has_failed or result.is_succeeded or result.is_skipped:
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
                        print(f"No task history found for {self.pipeline_id} {f' and with logical date {logical_date}' if logical_date else ''} yet")
                        time.sleep(5)
                        return check_status(result)
            status = check_status(SnowflakeDagResult(tasks = list(map(lambda task: SnowflakeTaskResult(name = task.full_name), self.dag.tasks))))
            if status.has_failed:
                raise StarlakeSnowflakeError(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} failed -> {status}")
            elif status.is_skipped:
                print(f"Pipeline {self.pipeline_id} {f'with logical date {logical_date}' if logical_date else ''} skipped -> {status}")
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

            datetime_format = '%Y-%m-%d %H:%M:%S %z'
            print(f" Pipeline {self.pipeline_id} is executing backfill job '{backfill_job_id}' with {partition_count} partition(s) from '{start_time.strftime(datetime_format)}' to '{end_time.strftime(datetime_format)}' using {interval} minutes interval")

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
    def is_skipped(self) -> bool:
        return self.state == 'SKIPPED'

    @property
    def is_succeeded(self) -> bool:
        return self.state == 'SUCCEEDED' 

    @property
    def state_as_str(self) -> str:
        if self.is_executing:
            return "is executing"
        return self.state.lower().replace('_', ' ')

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
    def is_skipped(self) -> bool:
        return any([task.is_skipped for task in self.tasks])

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
