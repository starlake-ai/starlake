from typing import List, Optional, Tuple, Union

from ai.starlake.common import MissingEnvironmentVariable

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, StarlakeExecutionEnvironment

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from snowflake.core.task import StoredProcedureCall
from snowflake.core.task.dagv1 import DAGTask

from snowflake.snowpark import Session

class SnowflakeEvent(AbstractEvent[StarlakeDataset]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> StarlakeDataset:
        return dataset

class StarlakeSnowflakeJob(IStarlakeJob[DAGTask, StarlakeDataset], StarlakeOptions, SnowflakeEvent):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self._stage_location = kwargs.get('stage_location', __class__.get_context_var(var_name='stage_location', options=self.options)) #stage_location is required
        try:
            self._warehouse = kwargs.get('warehouse', __class__.get_context_var(var_name='warehouse', options=self.options))
        except MissingEnvironmentVariable:
            self._warehouse = None
        packages = kwargs.get('packages', __class__.get_context_var(var_name='packages', default_value='croniter,python-dateutil', options=self.options)).split(',')
        packages = set([package.strip() for package in packages])
        packages.update(['croniter', 'python-dateutil', 'snowflake-snowpark-python'])
        self._packages = list(packages)
        timezone = kwargs.get('timezone', __class__.get_context_var(var_name='timezone', default_value='UTC', options=self.options))
        self._timezone = timezone
        try:
            self._sl_incoming_file_stage = kwargs.get('sl_incoming_file_stage', __class__.get_context_var(var_name='sl_incoming_file_stage', options=self.options))
        except MissingEnvironmentVariable:
            self._sl_incoming_file_stage = None

    @property
    def stage_location(self) -> Optional[str]:
        return self._stage_location

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    @property
    def packages(self) -> List[str]:
        return self._packages

    @property
    def timezone(self) -> str:
        return self._timezone

    @property
    def sl_incoming_file_stage(self) -> Optional[str]:
        return self._sl_incoming_file_stage

    @classmethod
    def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str]:
         return StarlakeOrchestrator.SNOWFLAKE

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.SQL

    def start_op(self, task_id, scheduled: bool, not_scheduled_datasets: Optional[List[StarlakeDataset]], least_frequent_datasets: Optional[List[StarlakeDataset]], most_frequent_datasets: Optional[List[StarlakeDataset]], **kwargs) -> Optional[DAGTask]:
        """Overrides IStarlakeJob.start_op()
        It represents the first task of a pipeline, it will define the optional condition that may trigger the DAG.
        Args:
            task_id (str): The required task id.
            scheduled (bool): whether the dag is scheduled or not.
            not_scheduled_datasets (Optional[List[StarlakeDataset]]): The optional not scheduled datasets.
            least_frequent_datasets (Optional[List[StarlakeDataset]]): The optional least frequent datasets.
            most_frequent_datasets (Optional[List[StarlakeDataset]]): The optional most frequent datasets.
        Returns:
            Optional[DAGTask]: The optional Snowflake task.
        """
        comment = kwargs.get('comment', f"dummy task for {task_id}")
        kwargs.update({'comment': comment})
        return super().start_op(task_id=task_id, scheduled=scheduled, not_scheduled_datasets=not_scheduled_datasets, least_frequent_datasets=least_frequent_datasets, most_frequent_datasets=most_frequent_datasets, **kwargs)

    def end_op(self, task_id: str, events: Optional[List[StarlakeDataset]] = None, **kwargs) -> Optional[DAGTask]:
        """Overrides IStarlakeJob.end_op()
        Generate a Snowflake task that will end the pipeline.
        """
        comment = kwargs.get('comment', f"end task for {task_id}")
        kwargs.update({'comment': comment})
        # TODO: implement the definition to update SL_START_DATE and SL_END_DATE if the backfill is enabled and the DAG is not scheduled - maybe we will have to retrieve all the dag runs that didn't execute successfully ?
        return super().end_op(task_id=task_id, events=events, **kwargs)

    def dummy_op(self, task_id: str, events: Optional[List[StarlakeDataset]] = None, **kwargs) -> DAGTask:
        """Dummy op.
        Generate a Snowflake dummy task.

        Args:
            task_id (str): The required task id.
            events (Optional[List[StarlakeDataset]]): The optional events to materialize.

        Returns:
            DAGTask: The Snowflake task.
        """
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"dummy task for {task_id}"
        kwargs.pop('comment', None)

        return DAGTask(
            name=task_id, 
            definition=f"select '{task_id}'", 
            comment=comment, 
            **kwargs
        )

    def skip_or_start_op(self, task_id: str, upstream_task: DAGTask, **kwargs) -> Optional[DAGTask]:
        """Overrides IStarlakeJob.skip_or_start_op()
        Generate a Snowflake task that will skip or start the pipeline.

        Args:
            task_id (str): The required task id.
            events (Optional[List[StarlakeDataset]]): The optional events to materialize.

        Returns:
            Optional[DAGTask]: The optional Snowflake task.
        """
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"skip or start task {task_id}"
        kwargs.pop('comment', None)

        def fun(session: Session, upstream_task_id: str) -> None:
            from snowflake.core.task.context import TaskContext
            context = TaskContext(session)
            return_value: str = context.get_predecessor_return_value(upstream_task_id)
            if return_value is None:
                print(f"upstream task {upstream_task_id} did not return any value")
                failed = True
            else:
                print(f"upstream task {upstream_task_id} returned {return_value}")
                try:
                    import ast
                    parsed_return_value = ast.literal_eval(return_value)
                    if isinstance(parsed_return_value, bool):
                        failed = not parsed_return_value
                    elif isinstance(parsed_return_value, int):
                        failed = parsed_return_value
                    elif isinstance(parsed_return_value, str) and parsed_return_value:
                        failed = int(parsed_return_value.strip())
                    else:
                        failed = True
                        print(f"Parsed return value {parsed_return_value}[{type(parsed_return_value)}] is not a valid bool, integer or is empty.")
                except (ValueError, SyntaxError) as e:
                    failed = True
                    print(f"Error parsing return value: {e}")
            if failed:
                raise ValueError(f"upstream task {upstream_task_id} failed")

        return DAGTask(
            name=task_id, 
            definition=StoredProcedureCall(
                func = fun,
                args=[upstream_task.name],
                stage_location=self.stage_location,
                packages=self.packages
            ), 
            comment=comment, 
            **kwargs
        )

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: Optional[StarlakeSparkConfig]=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_load()
        Generate the Snowflake task that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id.
            domain (str): The required domain of the table to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Union[StarlakeDataset, str]): The optional dataset to materialize.

        Returns:
            DAGTask: The Snowflake task.
        """
        if dataset:
            if isinstance(dataset, str):
                sink = dataset
            else:
                sink = dataset.sink
        else:
            sink = f"{domain}.{table}"
        kwargs.update({'sink': sink})
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"Starlake load {sink}"
        kwargs.update({'comment': comment})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Snowflake task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            DAGTask: The Snowflake task.
        """
        if dataset:
            if isinstance(dataset, str):
                sink = dataset
            else:
                sink = dataset.sink
        else:
            params = kwargs.get('params', dict())
            sink = params.get('sink', kwargs.get('sink', transform_name))
        kwargs.update({'sink': sink})
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"Starlake transform {sink}"
        kwargs.update({'comment': comment})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_job()
        Generate the Snowflake task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            DAGTask: The Snowflake task.
        """
        sink = kwargs.get('sink', None)
        command = arguments[0].lower()
        if sink:
            kwargs.pop('sink', None)
            domainAndTable = sink.split('.')
            domain = domainAndTable[0]
            table = domainAndTable[-1]
            statements = self.caller_globals.get('statements', dict()).get(sink, None)
            audit = self.caller_globals.get('audit', dict())
            expectations = self.caller_globals.get('expectations', dict())
            expectation_items = self.caller_globals.get('expectation_items', dict()).get(sink, None)
            comment = kwargs.get('comment', f'Starlake {sink} task')
            kwargs.pop('comment', None)
            options = self.sl_env_vars.copy() # Copy the current sl env variables
            from collections import defaultdict
            safe_params = defaultdict(lambda: 'NULL', options)
            for index, arg in enumerate(arguments):
                if arg == "--options" and arguments.__len__() > index + 1:
                    opts = arguments[index+1]
                    if opts.strip().__len__() > 0:
                        options.update({
                            key: value
                            for opt in opts.split(",")
                            if "=" in opt  # Only process valid key=value pairs
                            for key, value in [opt.split("=")]
                        })
                    break

            def bindParams(stmt: str) -> str:
                """Bind parameters to the SQL statement.
                Args:
                    stmt (str): The SQL statement.
                Returns:
                    str: The SQL statement with the parameters bound
                """
                return stmt.format_map(safe_params)

            def str_to_bool(value: str) -> bool:
                """Convert a string to a boolean.
                Args:
                    value (str): The string to convert.
                Returns:
                    bool: The boolean value.
                """
                truthy = {'yes', 'y', 'true', '1'}
                falsy = {'no', 'n', 'false', '0'}

                value = value.strip().lower()
                if value in truthy:
                    return True
                elif value in falsy:
                    return False
                raise ValueError(f"Valeur invalide : {value}")

            from snowflake.snowpark.dataframe import DataFrame
            from snowflake.snowpark.row import Row

            def execute_sql(session: Session, sql: Optional[str], message: Optional[str] = None, dry_run: bool = False) -> List[Row]:
                """Execute the SQL.
                Args:
                    session (Session): The Snowflake session.
                    sql (str): The SQL query to execute.
                    message (Optional[str], optional): The optional message. Defaults to None.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                Returns:
                    List[Row]: The rows.
                """
                if sql:
                    if dry_run and message:
                        print(f"-- {message}")
                    stmt: str = bindParams(sql)
                    if dry_run:
                        print(f"{stmt};")
                        return []
                    else:
                        try:
                            df: DataFrame = session.sql(stmt)
                            rows = df.collect()
                            return rows
                        except Exception as e:
                            raise Exception(f"Error executing SQL {stmt}: {str(e)}")
                else:
                    return []

            def execute_sqls(session: Session, sqls: List[str], message: Optional[str] = None, dry_run: bool = False) -> None:
                """Execute the SQLs.
                Args:
                    session (Session): The Snowflake session.
                    sqls (List[str]): The SQLs.
                    message (Optional[str], optional): The optional message. Defaults to None.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                if sqls:
                    if dry_run and message:
                        print(f"-- {message}")
                    for sql in sqls:
                        execute_sql(session, sql, None, dry_run)

            def check_if_table_exists(session: Session, domain: str, table: str) -> bool:
                """Check if the table exists.
                Args:
                    session (Session): The Snowflake session.
                    domain (str): The domain.
                    table (str): The table.
                    Returns:
                    bool: True if the table exists, False otherwise.
                """
                query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ILIKE '{domain}.{table}'"
                return execute_sql(session, query, f"Check if table {domain}.{table} exists:", False).__len__() > 0

            def check_if_audit_table_exists(session: Session, dry_run: bool = False) -> bool:
                """Check if the audit table exists.
                Args:
                    session (Session): The Snowflake session.
                    Returns:
                    bool: True if the audit table exists, False otherwise.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                if audit:
                    try:
                        # create SQL domain
                        domain = audit.get('domain', ['audit'])[0]
                        create_domain_if_not_exists(session, domain, dry_run)
                        # execute SQL preActions
                        execute_sqls(session, audit.get('preActions', []), "Execute audit pre action:", dry_run)
                        # check if the audit table exists
                        if not check_if_table_exists(session, domain, 'audit'):
                            # execute SQL createSchemaSql
                            sqls: List[str] = audit.get('createSchemaSql', [])
                            if sqls:
                                execute_sqls(session, sqls, "Create audit table", dry_run)
                            return True
                        else:
                            return True
                    except Exception as e:
                        print(f"Error creating audit table: {str(e)}")
                        return False
                else:
                    return False

            def check_if_expectations_table_exists(session: Session, dry_run: bool = False) -> bool:
                """Check if the expectations table exists.
                Args:
                    session (Session): The Snowflake session.
                    Returns:
                    bool: True if the expectations table exists, False otherwise.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                if expectations:
                    try:
                        # create SQL domain
                        domain = expectations.get('domain', ['audit'])[0]
                        create_domain_if_not_exists(session, domain, dry_run)
                        # check if the expectations table exists
                        if not check_if_table_exists(session, domain, 'expectations'):
                            # execute SQL createSchemaSql
                            execute_sqls(session, expectations.get('createSchemaSql', []), "Create expectations table", dry_run)
                            return True
                        else:
                            return True
                    except Exception as e:
                        print(f"Error creating expectations table: {str(e)}")
                        return False
                else:
                    return False

            from datetime import datetime

            def log_audit(session: Session, paths: Optional[str], count: int, countAccepted: int, countRejected: int, success: bool, duration: int, message: str, ts: datetime, jobid: Optional[str] = None, step: Optional[str] = None, dry_run: bool = False) -> bool :
                """Log the audit record.
                Args:
                    session (Session): The Snowflake session.
                    count (int): The count.
                    countAccepted (int): The count accepted.
                    countRejected (int): The count rejected.
                    success (bool): The success.
                    duration (int): The duration.
                    message (str): The message.
                    ts (datetime): The timestamp.
                    jobid (Optional[str], optional): The optional job id. Defaults to None.
                    step (Optional[str], optional): The optional step. Defaults to None.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                Returns:
                    bool: True if the audit record was logged, False otherwise.
                """
                if audit and check_if_audit_table_exists(session, dry_run):
                    audit_domain = audit.get('domain', ['audit'])[0]
                    audit_sqls = audit.get('mainSqlIfExists', None)
                    if audit_sqls:
                        try:
                            audit_sql = audit_sqls[0]
                            formatted_sql = audit_sql.format(
                                jobid = jobid or f'{domain}.{table}',
                                paths = paths or table,
                                domain = domain,
                                schema = table,
                                success = str(success),
                                count = str(count),
                                countAccepted = str(countAccepted),
                                countRejected = str(countRejected),
                                timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                                duration = str(duration),
                                message = message,
                                step = step or "TRANSFORM",
                                database = "",
                                tenant = ""
                            )
                            insert_sql = f"INSERT INTO {audit_domain}.audit {formatted_sql}"
                            execute_sql(session, insert_sql, "Insert audit record:", dry_run)
                            return True
                        except Exception as e:
                            print(f"Error inserting audit record: {str(e)}")
                            return False
                    else:
                        return False
                else:
                    return False

            def log_expectation(session: Session, success: bool, name: str, params: str, sql: str, count: int, exception: str, ts: datetime, jobid: Optional[str] = None, dry_run: bool = False) -> bool :
                """Log the expectation record.
                Args:
                    session (Session): The Snowflake session.
                    success (bool): whether the expectation has been successfully checked or not.
                    name (str): The name of the expectation.
                    params (str): The params for the expectation.
                    sql (str): The SQL.
                    count (int): The count.
                    exception (str): The exception.
                    ts (datetime): The timestamp.
                    jobid (Optional[str], optional): The optional job id. Defaults to None.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                Returns:
                    bool: True if the expectation record was logged, False otherwise.
                """
                if expectations and check_if_expectations_table_exists(session, dry_run):
                    expectation_domain = expectations.get('domain', ['audit'])[0]
                    expectation_sqls = expectations.get('mainSqlIfExists', None)
                    if expectation_sqls:
                        try:
                            expectation_sql = expectation_sqls[0]
                            formatted_sql = expectation_sql.format(
                                jobid = jobid or f'{domain}.{table}',
                                database = "",
                                domain = domain,
                                schema = table,
                                count = count,
                                exception = exception,
                                timestamp = ts.strftime("%Y-%m-%d %H:%M:%S"),
                                success = str(success),
                                name = name,
                                params = params,
                                sql = sql
                            )
                            insert_sql = f"INSERT INTO {expectation_domain}.expectations {formatted_sql}"
                            execute_sql(session, insert_sql, "Insert expectations record:", dry_run)
                            return True
                        except Exception as e:
                            print(f"Error inserting expectations record: {str(e)}")
                            return False
                    else:
                        return False
                else:
                    return False

            def run_expectation(session: Session, name: str, params: str, query: str, failOnError: bool = False, jobid: Optional[str] = None, dry_run: bool = False) -> None:
                """Run the expectation.
                Args:
                    session (Session): The Snowflake session.
                    name (str): The name of the expectation.
                    params (str): The params for the expectation.
                    query (str): The query.
                    failOnError (bool, optional): Whether to fail on error. Defaults to False.
                    jobid (Optional[str], optional): The optional job id. Defaults to None.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                count = 0
                try:
                    if query:
                        rows = execute_sql(session, query, f"Run expectation {name}:", dry_run)
                        if rows.__len__() != 1:
                            if not dry_run:
                                raise Exception(f'Expectation failed for {sink}: {query}. Expected 1 row but got {rows.__len__()}')
                        else:
                            count = rows[0][0]
                        #  log expectations as audit in expectation table here
                        if count != 0:
                            raise Exception(f'Expectation failed for {sink}: {query}. Expected count to be equal to 0 but got {count}')
                        log_expectation(session, True, name, params, query, count, "", datetime.now(), jobid, dry_run)
                    else:
                        raise Exception(f'Expectation failed for {sink}: {name}. Query not found')
                except Exception as e:
                    print(f"Error running expectation {name}: {str(e)}")
                    log_expectation(session, False, name, params, query, count, str(e), datetime.now(), jobid, dry_run)
                    if failOnError and not dry_run:
                        raise e

            def run_expectations(session: Session, jobid: Optional[str] = None, dry_run: bool = False) -> None:
                """Run the expectations.
                Args:
                    session (Session): The Snowflake session.
                    jobid (Optional[str], optional): The optional job id. Defaults to None.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                if expectation_items and check_if_expectations_table_exists(session, dry_run):
                    for expectation in expectation_items:
                        run_expectation(session, expectation.get("name", None), expectation.get("params", None), expectation.get("query", None), str_to_bool(expectation.get('failOnError', 'no')), jobid, dry_run)

            def begin_transaction(session: Session, dry_run: bool = False) -> None:
                """Begin the transaction.
                Args:
                    session (Session): The Snowflake session.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                execute_sql(session, "BEGIN", "BEGIN transaction:", dry_run)

            def create_domain_if_not_exists(session: Session, domain: str, dry_run: bool = False) -> None:
                """Create the schema if it does not exist.
                Args:
                    session (Session): The Snowflake session.
                    domain (str): The domain.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                execute_sql(session, f"CREATE SCHEMA IF NOT EXISTS {domain}", f"Create schema {domain} if not exists:", dry_run)

            def enable_change_tracking(session: Session, sink: str, dry_run: bool = False) -> None:
                """Enable change tracking.
                Args:
                    session (Session): The Snowflake session.
                    sink (str): The sink.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                execute_sql(session, f"ALTER TABLE {sink} SET CHANGE_TRACKING = TRUE", "Enable change tracking:", dry_run)

            def commit_transaction(session: Session, dry_run: bool = False) -> None:
                """Commit the transaction.
                Args:
                    session (Session): The Snowflake session.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                execute_sql(session, "COMMIT", "COMMIT transaction:", dry_run)

            def rollback_transaction(session: Session, dry_run: bool = False) -> None:
                """Rollback the transaction.
                Args:
                    session (Session): The Snowflake session.
                    dry_run (bool, optional): Whether to run in dry run mode. Defaults to False.
                """
                execute_sql(session, "ROLLBACK", "ROLLBACK transaction:", dry_run)

            if command == 'transform':
                if statements:

                    cron_expr = kwargs.get('cron_expr', None)
                    kwargs.pop('cron_expr', None)

                    format = '%Y-%m-%d %H:%M:%S%z'

                    # create the function that will execute the transform
                    def fun(session: Session, dry_run: bool) -> None:
                        from datetime import datetime

                        if dry_run:
                            print(f"-- Executing transform for {sink} in dry run mode")

                        if cron_expr:
                            from croniter import croniter
                            from croniter.croniter import CroniterBadCronError
                            # get the original scheduled timestamp of the initial graph run in the current group
                            # For graphs that are retried, the returned value is the original scheduled timestamp of the initial graph run in the current group.
                            if dry_run:
                                config = None
                            else:
                                config = session.call("system$get_task_graph_config")
                            if config:
                                import json
                                config = json.loads(config)
                            else:
                                config = {}
                            original_schedule = config.get("logical_date", None)
                            if not original_schedule:
                                query = "SELECT to_timestamp(system$task_runtime_info('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP'))"
                                print(f"-- Get the original scheduled timestamp of the initial graph run:\n{query};")
                                rows = execute_sql(session, query, "Get the original scheduled timestamp of the initial graph run", dry_run)
                                if rows.__len__() == 1:
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
                            try:
                                croniter(cron_expr)
                                iter = croniter(cron_expr, start_time)
                                curr = iter.get_current(datetime)
                                previous = iter.get_prev(datetime)
                                next = croniter(cron_expr, previous).get_next(datetime)
                                if curr == next :
                                    sl_end_date = curr
                                else:
                                    sl_end_date = previous
                                sl_start_date = croniter(cron_expr, sl_end_date).get_prev(datetime)
                                safe_params.update({'sl_start_date': sl_start_date.strftime(format), 'sl_end_date': sl_end_date.strftime(format)})
                            except CroniterBadCronError:
                                raise ValueError(f"Invalid cron expression: {cron_expr}")

                        if dry_run:
                            jobid = sink
                        else:
                            jobid = str(session.call("system$current_user_task_name"))

                        start = datetime.now()

                        try:
                            # BEGIN transaction
                            begin_transaction(session, dry_run)

                            # create SQL domain
                            create_domain_if_not_exists(session, domain, dry_run)

                            # execute preActions
                            execute_sqls(session, statements.get('preActions', []), "Pre actions", dry_run)

                            # execute preSqls
                            execute_sqls(session, statements.get('preSqls', []), "Pre sqls", dry_run)

                            if check_if_table_exists(session, domain, table):
                                # enable change tracking
                                enable_change_tracking(session, sink, dry_run)
                                # execute addSCD2ColumnsSqls
                                execute_sqls(session, statements.get('addSCD2ColumnsSqls', []), "Add SCD2 columns", dry_run)
                                # execute mainSqlIfExists
                                execute_sqls(session, statements.get('mainSqlIfExists', []), "Main sql if exists", dry_run)
                            else:
                                # execute mainSqlIfNotExists
                                execute_sqls(session, statements.get('mainSqlIfNotExists', []), "Main sql if not exists", dry_run)
                                # enable change tracking
                                enable_change_tracking(session, sink, dry_run)

                            # execute postSqls
                            execute_sqls(session, statements.get('postSqls', []) , "Post sqls", dry_run)

                            # run expectations
                            run_expectations(session, jobid, dry_run)

                            # COMMIT transaction
                            commit_transaction(session, dry_run)
                            end = datetime.now()
                            duration = (end - start).total_seconds()
                            print(f"-- Duration in seconds: {duration}")
                            log_audit(session, None, -1, -1, -1, True, duration, 'Success', end, jobid, "TRANSFORM", dry_run)
                            
                        except Exception as e:
                            # ROLLBACK transaction
                            error_message = str(e)
                            print(f"-- Error executing transform for {sink}: {error_message}")
                            rollback_transaction(session, dry_run)
                            end = datetime.now()
                            duration = (end - start).total_seconds()
                            print(f"-- Duration in seconds: {duration}")
                            log_audit(session, None, -1, -1, -1, False, duration, error_message, end, jobid, "TRANSFORM", dry_run)
                            raise e

                    kwargs.pop('params', None)
                    kwargs.pop('events', None)

                    return DAGTask(
                        name=task_id, 
                        definition=StoredProcedureCall(
                            func = fun,
                            args=[False], 
                            stage_location=self.stage_location,
                            packages=self.packages,
                        ), 
                        comment=comment, 
                        **kwargs
                    )
                else:
                    # sink statements are required
                    raise ValueError(f"Transform '{sink}' statements not found")
            elif command == 'load':
                json_context = self.caller_globals.get('json_context', None)
                if json_context:
                    import json
                    context = json.loads(json_context).get(sink, None)
                    if context:
                        temp_stage = self.sl_incoming_file_stage or context.get('tempStage', None)
                        if not temp_stage:
                            raise ValueError(f"Temp stage for {sink} not found")
                        context_schema: dict = context.get('schema', dict())
                        pattern: str = context_schema.get('pattern', None)
                        if not pattern:
                            raise ValueError(f"Pattern for {sink} not found")
                        metadata: dict = context_schema.get('metadata', dict())
                        format: str = metadata.get('format', None)
                        if not format:
                            raise ValueError(f"Format for {sink} not found")
                        else:
                            format = format.upper()
                        metadata_options: dict = metadata.get("options", dict())

                        def get_option(key: str, metadata_key: Optional[str]) -> Optional[str]:
                            if metadata_options and key.lower() in metadata_options:
                                return metadata_options.get(key.lower(), None)
                            elif metadata_key and metadata.get(metadata_key, None):
                                return metadata[metadata_key].replace('\\', '\\\\')
                            return None

                        def is_true(value: str, default: bool) -> bool:
                            if value is None:
                                return default
                            return value.lower() == "true"

                        def get_audit_info(rows: List[Row]) -> Tuple[str, str, str, int, int, int]:
                            if rows.__len__() == 0:
                                return '', '', '', -1, -1, -1
                            else:
                                files = []
                                first_error_lines = []
                                first_error_column_names = []
                                rows_parsed = 0
                                rows_loaded = 0
                                errors_seen = 0
                                for row in rows:
                                    row_dict = row.as_dict()
                                    file = row_dict.get('file', None)
                                    if file:
                                        files.append(file)
                                    first_error_line=row_dict.get('first_error_line', None)
                                    if first_error_line:
                                        first_error_lines.append(first_error_line)
                                    first_error_column_name=row_dict.get('first_error_column_name', None)
                                    if first_error_column_name:
                                        first_error_column_names.append(first_error_column_name)
                                    rows_parsed += row_dict.get('rows_parsed', 0)
                                    rows_loaded += row_dict.get('rows_loaded', 0)
                                    errors_seen += row_dict.get('errors_seen', 0)
                                return ','.join(files), ','.join(first_error_lines), ','.join(first_error_column_names), rows_parsed, rows_loaded, errors_seen

                        def copy_extra_options(common_options: list[str]):
                            extra_options = ""
                            if metadata_options:
                                for k, v in metadata_options.items():
                                    if not k in common_options:
                                        extra_options += f"{k} = {v}\n"
                            return extra_options

                        def schema_as_dict(schema_string: str) -> dict:
                            tableNativeSchema = map(lambda x: (x.split()[0].strip(), x.split()[1].strip()), schema_string.replace("\"", "").split(","))
                            tableSchemaDict = dict(map(lambda x: (x[0].lower(), x[1].lower()), tableNativeSchema))
                            return tableSchemaDict

                        def add_columns_from_dict(dictionary: dict):
                            return [f"ALTER TABLE IF EXISTS {domain}.{table} ADD COLUMN IF NOT EXISTS {k} {v};" for k, v in dictionary.items()]
    
                        def drop_columns_from_dict(dictionary: dict):
                            # In the current version, we do not drop any existing columns for backward compatibility
                            # return [f"ALTER TABLE IF EXISTS {domain}.{table} DROP COLUMN IF EXISTS {k};" for k, v in dictionary.items()]
                            return []    

                        def update_table_schema(session: Session, dry_run: bool) -> bool:
                            existing_schema_sql = f"select column_name, data_type from information_schema.columns where table_schema ilike '{domain}' and table_name ilike '{table}';"
                            rows = execute_sql(session, existing_schema_sql, f"Retrieve existing schema for {domain}.{table}", False)
                            existing_columns = []
                            for row in rows:
                                existing_columns.append((str(row[0]).lower(), str(row[1]).lower()))
                            existing_schema = dict(existing_columns)
                            if dry_run:
                                print(f"-- Existing schema for {domain}.{table}: {existing_schema}")
                            schema_string = statements.get("schemaString", "") 
                            if schema_string.strip() == "":
                                return False
                            new_schema = schema_as_dict(schema_string)
                            new_columns = set(new_schema.keys()) - set(existing_schema.keys())
                            old_columns = set(existing_schema.keys()) - set(new_schema.keys())
                            nb_new_columns = new_columns.__len__()
                            nb_old_columns = old_columns.__len__()
                            update_required = nb_new_columns + nb_old_columns > 0
                            if not update_required:
                                if dry_run:
                                    print(f"-- No schema update required for {domain}.{table}")
                                return False
                            new_columns_dict = {key: new_schema[key] for key in new_columns}
                            old_columns_dict = {key: existing_schema[key] for key in old_columns}
                            alter_columns = add_columns_from_dict(new_columns_dict)
                            execute_sqls(session, alter_columns, "Add columns", dry_run)

                            old_columns_dict = {key: existing_schema[key] for key in old_columns}
                            drop_columns = drop_columns_from_dict(old_columns_dict)
                            execute_sqls(session, drop_columns, "Drop columns", dry_run)

                            return True

                        compression = is_true(get_option("compression", None), True)
                        if compression:
                            compression_format = "COMPRESSION = GZIP" 
                        else:
                            compression_format = "COMPRESSION = NONE"

                        null_if = get_option('NULL_IF', None)
                        if not null_if and is_true(metadata.get('emptyIsNull', "false"), False):
                            null_if = "NULL_IF = ('')"
                        elif null_if:
                            null_if = f"NULL_IF = {null_if}"
                        else:
                            null_if = ""

                        purge = get_option("PURGE", None)
                        if not purge:
                            purge = "FALSE"
                        else:
                            purge = purge.upper()

                        def build_copy_csv() -> str:
                            skipCount = get_option("SKIP_HEADER", None)

                            if not skipCount and is_true(metadata.get('withHeader', 'false'), False):
                                skipCount = '1'

                            common_options = [
                                'SKIP_HEADER', 
                                'NULL_IF', 
                                'FIELD_OPTIONALLY_ENCLOSED_BY', 
                                'FIELD_DELIMITER',
                                'ESCAPE_UNENCLOSED_FIELD', 
                                'ENCODING'
                            ]
                            extra_options = copy_extra_options(common_options)
                            if compression:
                                extension = ".gz"
                            else:
                                extension = ""
                            sql = f'''
                                COPY INTO {sink} 
                                FROM @{temp_stage}/{domain}/
                                PATTERN = '{pattern}{extension}'
                                PURGE = {purge}
                                FILE_FORMAT = (
                                    TYPE = CSV
                                    ERROR_ON_COLUMN_COUNT_MISMATCH = false
                                    SKIP_HEADER = {skipCount} 
                                    FIELD_OPTIONALLY_ENCLOSED_BY = '{get_option('FIELD_OPTIONALLY_ENCLOSED_BY', 'quote')}' 
                                    FIELD_DELIMITER = '{get_option('FIELD_DELIMITER', 'separator')}' 
                                    ESCAPE_UNENCLOSED_FIELD = '{get_option('ESCAPE_UNENCLOSED_FIELD', 'escape')}' 
                                    ENCODING = '{get_option('ENCODING', 'encoding')}'
                                    {null_if}
                                    {extra_options}
                                    {compression_format}
                                )
                            '''
                            return sql

                        def build_copy_json() -> str:
                            strip_outer_array = get_option("STRIP_OUTER_ARRAY", 'array')
                            common_options = [
                                'STRIP_OUTER_ARRAY', 
                                'NULL_IF'
                            ]
                            extra_options = copy_extra_options(common_options)
                            sql = f'''
                                COPY INTO {sink} 
                                FROM @{temp_stage}/{domain}
                                PATTERN = '{pattern}'
                                PURGE = {purge}
                                FILE_FORMAT = (
                                    TYPE = JSON
                                    STRIP_OUTER_ARRAY = {strip_outer_array}
                                    {null_if}
                                    {extra_options}
                                    {compression_format}
                                )
                            '''
                            return sql
                            
                        def build_copy_other(format: str) -> str:
                            common_options = [
                                'NULL_IF'
                            ]
                            extra_options = copy_extra_options(common_options)
                            sql = f'''
                                COPY INTO {sink} 
                                FROM @{temp_stage}/{domain} 
                                PATTERN = '{pattern}'
                                PURGE = {purge}
                                FILE_FORMAT = (
                                    TYPE = {format}
                                    {null_if}
                                    {extra_options}
                                    {compression_format}
                                )
                            '''
                            return sql

                        def build_copy() -> str:
                            if format == 'DSV':
                                return build_copy_csv()
                            elif format == 'JSON':
                                return build_copy_json()
                            elif format == 'PARQUET':
                                return build_copy_other()
                            elif format == 'XML':
                                return build_copy_other()
                            else:
                                raise ValueError(f"Unsupported format {format}")
  
                        # create the function that will execute the load
                        def fun(session: Session, dry_run: bool) -> None:
                            from datetime import datetime

                            if dry_run:
                                jobid = sink
                            else:
                                jobid = str(session.call("system$current_user_task_name"))

                            start = datetime.now()

                            try:
                                # BEGIN transaction
                                begin_transaction(session, dry_run)

                                nbSteps = int(statements.get('steps', '1'))
                                write_strategy = statements.get('writeStrategy', None)
                                if nbSteps == 1:
                                    # execute schema presql
                                    execute_sqls(session, context_schema.get('presql', []), "Pre sqls", dry_run)
                                    # create table
                                    execute_sqls(session, statements.get('createTable', []), "Create table", dry_run)
                                    exists = check_if_table_exists(session, domain, table)
                                    if exists:
                                        # enable change tracking
                                        enable_change_tracking(session, sink, dry_run)
                                        # update table schema
                                        update_table_schema(session, dry_run)
                                    if write_strategy == 'WRITE_TRUNCATE':
                                        # truncate table
                                        execute_sql(session, f"TRUNCATE TABLE {sink}", "Truncate table", dry_run)
                                    # create stage if not exists
                                    execute_sql(session, f"CREATE STAGE IF NOT EXISTS {temp_stage}", "Create stage", dry_run)
                                    # copy data
                                    copy_results = execute_sql(session, build_copy(), "Copy data", dry_run)
                                    if not exists:
                                        # enable change tracking
                                        enable_change_tracking(session, sink, dry_run)
                                elif nbSteps == 2:
                                    # execute first step
                                    execute_sqls(session, statements.get('firstStep', []), "Execute first step", dry_run)
                                    if write_strategy == 'WRITE_TRUNCATE':
                                        # truncate table
                                        execute_sql(session, f"TRUNCATE TABLE {sink}", "Truncate table", dry_run)
                                    # create stage if not exists
                                    execute_sql(session, f"CREATE STAGE IF NOT EXISTS {temp_stage}", "Create stage", dry_run)
                                    # copy data
                                    copy_results = execute_sql(session, build_copy(), "Copy data", dry_run)
                                    second_step = statements.get('secondStep', dict())
                                    # execute preActions
                                    execute_sqls(session, second_step.get('preActions', []), "Pre actions", dry_run)
                                    # execute schema presql
                                    execute_sqls(session, context_schema.get('presql', []), "Pre sqls", dry_run)
                                    if check_if_table_exists(session, domain, table):
                                        # enable change tracking
                                        enable_change_tracking(session, sink, dry_run)
                                        # execute addSCD2ColumnsSqls
                                        execute_sqls(session, second_step.get('addSCD2ColumnsSqls', []), "Add SCD2 columns", dry_run)
                                        # update schema
                                        update_table_schema(session, dry_run)
                                        # execute mainSqlIfExists
                                        execute_sqls(session, second_step.get('mainSqlIfExists', []), "Main sql if exists", dry_run)
                                    else:
                                        # execute mainSqlIfNotExists
                                        execute_sqls(session, second_step.get('mainSqlIfNotExists', []), "Main sql if not exists", dry_run)
                                        # enable change tracking
                                        enable_change_tracking(session, sink, dry_run)
                                    # execute dropFirstStep
                                    execute_sql(session, statements.get('dropFirstStep', None), "Drop first step", dry_run)
                                else:
                                    raise ValueError(f"Invalid number of steps: {nbSteps}")

                                # execute schema postsql
                                execute_sqls(session, context_schema.get('postsql', []), "Post sqls", dry_run)

                                # run expectations
                                run_expectations(session, jobid, dry_run)

                                # COMMIT transaction
                                commit_transaction(session, dry_run)
                                end = datetime.now()
                                duration = (end - start).total_seconds()
                                print(f"-- Duration in seconds: {duration}")
                                files, first_error_line, first_error_column_name, rows_parsed, rows_loaded, errors_seen = get_audit_info(copy_results)
                                message = first_error_line + '\n' + first_error_column_name
                                success = errors_seen == 0
                                log_audit(session, files, rows_parsed, rows_loaded, errors_seen, success, duration, message, end, jobid, "LOAD", dry_run)
                                
                            except Exception as e:
                                # ROLLBACK transaction
                                error_message = str(e)
                                print(f"-- Error executing load for {sink}: {error_message}")
                                rollback_transaction(session, dry_run)
                                end = datetime.now()
                                duration = (end - start).total_seconds()
                                print(f"-- Duration in seconds: {duration}")
                                log_audit(session, None, -1, -1, -1, False, duration, error_message, end, jobid, "LOAD", dry_run)
                                raise e

                        kwargs.pop('params', None)
                        kwargs.pop('events', None)

                        return DAGTask(
                            name=task_id, 
                            definition=StoredProcedureCall(
                                func = fun,
                                args=[False], 
                                stage_location=self.stage_location,
                                packages=self.packages,
                            ), 
                            comment=comment, 
                            **kwargs
                        )
                    else:
                        raise ValueError(f"Context for {sink} not found")
                else:
                    raise ValueError("context is required")
            else:
                # only load and transform commands are implemented
                raise NotImplementedError(f"{command} is not implemented")
        else:
            # sink is required
            raise ValueError("sink is required")
