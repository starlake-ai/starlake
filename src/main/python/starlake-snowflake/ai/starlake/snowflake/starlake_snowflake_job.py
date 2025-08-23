from typing import List, Optional, Tuple, Union

from ai.starlake.common import MissingEnvironmentVariable

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, StarlakeExecutionEnvironment, TaskType

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from ai.starlake.helper import zip_selected_packages

from snowflake.core.task import StoredProcedureCall
from snowflake.core.task.dagv1 import DAGTask

from snowflake.snowpark import Session

from datetime import datetime

class SnowflakeEvent(AbstractEvent[StarlakeDataset]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> StarlakeDataset:
        return dataset

class StarlakeSnowflakeJob(IStarlakeJob[DAGTask, StarlakeDataset], StarlakeOptions, SnowflakeEvent):
    def __init__(self, filename: str=None, module_name: str=None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self._stage_location = kwargs.get('stage_location', __class__.get_context_var(var_name='stage_location', options=self.options)) #stage_location is required
        try:
            self._warehouse = kwargs.get('warehouse', __class__.get_context_var(var_name='warehouse', options=self.options))
        except MissingEnvironmentVariable:
            self._warehouse = None
        packages = kwargs.get('packages', __class__.get_context_var(var_name='packages', default_value='croniter,python-dateutil', options=self.options)).split(',')
        packages = set([package.strip() for package in packages])
        packages.update(['croniter', 'python-dateutil', 'snowflake-snowpark-python'])
        self.__packages = list(packages)
        try:
            self.__sl_incoming_file_stage = kwargs.get('sl_incoming_file_stage', __class__.get_context_var(var_name='sl_incoming_file_stage', options=self.options))
        except MissingEnvironmentVariable:
            self.__sl_incoming_file_stage = None
        allow_overlapping_execution: bool = kwargs.get('allow_overlapping_execution', __class__.get_context_var(var_name='allow_overlapping_execution', default_value='False', options=self.options).lower() == 'true')
        self.__allow_overlapping_execution = allow_overlapping_execution
        self.__ai_zip = zip_selected_packages()
        self.pipeline_id = self.caller_filename.replace(".py", "").replace(".pyc", "").upper() #TODO add to generic starlake job

    @property
    def stage_location(self) -> Optional[str]:
        return self._stage_location

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    @property
    def packages(self) -> List[str]:
        return self.__packages

    @property
    def allow_overlapping_execution(self) -> bool:
        return self.__allow_overlapping_execution

    @property
    def sl_incoming_file_stage(self) -> Optional[str]:
        return self.__sl_incoming_file_stage

    @property
    def ai_zip(self) -> str:
        return self.__ai_zip

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
        # this condition will be used to check if all the datasets are present or not
        # its value will be set by the upstream task to the logical date of the running dag
        kwargs.update({'condition': "SYSTEM$GET_PREDECESSOR_RETURN_VALUE() <> ''"})
        return super().start_op(task_id=task_id, scheduled=scheduled, not_scheduled_datasets=not_scheduled_datasets, least_frequent_datasets=least_frequent_datasets, most_frequent_datasets=most_frequent_datasets, **kwargs)

    def end_op(self, task_id: str, events: Optional[List[StarlakeDataset]] = None, **kwargs) -> Optional[DAGTask]:
        """Overrides IStarlakeJob.end_op()
        Generate a Snowflake task that will end the pipeline.
        """
        comment = kwargs.get('comment', f"end task for {task_id}")
        kwargs.update({'comment': comment})
        # TODO: implement the definition to update SL_START_DATE and SL_END_DATE if the backfill is enabled and the DAG is not scheduled - maybe we will have to retrieve all the dag runs that didn't execute successfully ?
        return super().end_op(task_id=task_id, events=events, **kwargs)

    def dummy_op(self, task_id: str, events: Optional[List[StarlakeDataset]] = None, task_type: Optional[TaskType]=TaskType.EMPTY, **kwargs) -> DAGTask:
        """Dummy op.
        Generate a Snowflake dummy task.

        Args:
            task_id (str): The required task id.
            events (Optional[List[StarlakeDataset]]): The optional events to materialize.
            task_type (Optional[TaskType]): The optional task type.

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
        logger = self.logger
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"skip or start task {task_id}"
        kwargs.pop('comment', None)

        def fun(session: Session, upstream_task_id: str) -> None:
            from snowflake.core.task.context import TaskContext
            context = TaskContext(session)
            return_value: str = context.get_predecessor_return_value(upstream_task_id)
            if return_value is None:
                logger.warning(f"upstream task {upstream_task_id} did not return any value")
                failed = True
            else:
                logger.info(f"upstream task {upstream_task_id} returned {return_value}")
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
                        logger.error(f"Parsed return value {parsed_return_value}[{type(parsed_return_value)}] is not a valid bool, integer or is empty.")
                except (ValueError, SyntaxError) as e:
                    failed = True
                    logger.error(f"Error parsing return value: {e}")
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
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"Starlake load {domain}.{table}"
        kwargs.update({'comment': comment})
        if self.run_dependencies_first:
            # if we run dependencies first, we will not load the dataset but just return a dummy task
            task_id = kwargs.get("task_id", f"load_{domain}_{table}") if not task_id else task_id
            kwargs.pop("task_id", None)
            kwargs.pop("params", None)
            return self.dummy_op(task_id=task_id, **kwargs)
        if dataset:
            if isinstance(dataset, str):
                sink = dataset
            else:
                sink = dataset.sink
        else:
            sink = f"{domain}.{table}"
        kwargs.update({'sink': sink})
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

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType]=None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_job()
        Generate the Snowflake task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType], optional): The optional task type. Defaults to None.

        Returns:
            DAGTask: The Snowflake task.
        """
        sink = kwargs.get('sink', None)
        if not task_type and len(arguments) > 0:
            task_type = TaskType.from_str(arguments[0])
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

            params = kwargs.get('params', {})
            cron_expr = params.get('cron', params.get('cronExpr', None))
            kwargs.pop('params', None)

            allow_overlapping_execution = self.allow_overlapping_execution

            if task_type == TaskType.TRANSFORM:
                from ai.starlake.helper import datetime_format, SnowflakeTaskHelper
                helper = SnowflakeTaskHelper(sink=sink, domain=domain, table=table, audit=audit, expectations=expectations, expectation_items=expectation_items, name=self.pipeline_id, timezone=self.timezone)

                safe_params = helper.safe_params

                info = helper.info
                error = helper.error

                begin_transaction = helper.begin_transaction
                commit_transaction = helper.commit_transaction
                rollback_transaction = helper.rollback_transaction

                execute_sql = helper.execute_sql
                execute_sqls = helper.execute_sqls

                get_task_logical_date = helper.get_task_logical_date
                as_datetime = helper.as_datetime
                get_start_end_dates = helper.get_start_end_dates

                check_if_dataset_exists = helper.check_if_dataset_exists
                create_domain_if_not_exists = helper.create_domain_if_not_exists

                log_audit = helper.log_audit
                get_audit_info = helper.get_audit_info

                run_expectations = helper.run_expectations

                update_table_schema = helper.update_table_schema

                if statements:

                    # create the function that will execute the transform
                    def fun(session: Session, dry_run: bool, logical_date: Optional[str] = None) -> None:
                        if dry_run:
                            print(f"-- Executing transform for {sink} in dry run mode")

                        backfill: bool = False
                        if allow_overlapping_execution:
                            query = "SELECT SYSTEM$TASK_RUNTIME_INFO('IS_BACKFILL')::boolean"
                            rows = execute_sql(session, query, "Check if the current running dag is a backfill", dry_run)
                            if rows.__len__() == 1:
                                backfill = rows[0][0]

                        if not logical_date:
                            logical_date = get_task_logical_date(session, backfill, dry_run=dry_run)
                        logical_date = as_datetime(logical_date)

                        if cron_expr and not backfill:
                            # if a cron expression has been provided, the scheduled date corresponds to the end date determined by applying the cron expression to the logical date
                            (_, scheduled_date) = get_start_end_dates(cron_expr, logical_date)
                        else:
                            scheduled_date = logical_date

                        sl_data_interval_start = None
                        sl_data_interval_end = None

                        if backfill:
                            # if backfill, the data interval start and end are the partition start and end dates
                            query = "SELECT SYSTEM$TASK_RUNTIME_INFO('PARTITION_START')::timestamp_ltz"
                            rows = execute_sql(session, query, "Get the partition start date", dry_run)
                            if rows.__len__() == 1:
                                partition_start = rows[0][0]
                            if partition_start:
                                if isinstance(partition_start, str):
                                    sl_data_interval_start = as_datetime(partition_start)
                                else:
                                    sl_data_interval_start = partition_start
                            sl_data_interval_end = logical_date

                        if cron_expr and (not sl_data_interval_start or not sl_data_interval_end):
                            # if cron expression is provided, calculate the start and end dates
                            (sl_data_interval_start, sl_data_interval_end) = get_start_end_dates(cron_expr, logical_date)

                        if sl_data_interval_start and sl_data_interval_end:
                            safe_params.update({'sl_data_interval_start': sl_data_interval_start.strftime(datetime_format), 'sl_data_interval_end': sl_data_interval_end.strftime(datetime_format)})

                        # get the current job id
                        query = "SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID')::string"
                        rows = execute_sql(session, query, "Get the current task graph run group id", dry_run)
                        if rows.__len__() == 1:
                            jobid = rows[0][0]
                        else:
                            jobid = sink

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

                            if check_if_dataset_exists(session, f"{domain}.{table}"):
                                # enable change tracking
                                # enable_change_tracking(session, sink, dry_run)
                                # update table schema
                                update_table_schema(session, schema_string=",".join(statements.get("targetSchema", [])), sync_strategy=statements.get("syncStrategy", None), dry_run=dry_run)
                                # execute addSCD2ColumnsSqls
                                execute_sqls(session, statements.get('addSCD2ColumnsSqls', []), "Add SCD2 columns", dry_run)
                                # execute mainSqlIfExists
                                execute_sqls(session, statements.get('mainSqlIfExists', []), "Main sql if exists", dry_run)
                            else:
                                # execute mainSqlIfNotExists
                                execute_sqls(session, statements.get('mainSqlIfNotExists', []), "Main sql if not exists", dry_run)
                                # enable change tracking
                                # enable_change_tracking(session, sink, dry_run)

                            # execute postsql
                            execute_sqls(session, statements.get('postsql', []) , "Post sqls", dry_run)

                            # run expectations
                            run_expectations(session, jobid, dry_run)

                            # COMMIT transaction
                            commit_transaction(session, dry_run)
                            end = datetime.now()
                            duration = (end - start).total_seconds()
                            info(f"Duration in seconds: {duration}", dry_run=dry_run)
                            log_audit(session, None, -1, -1, -1, True, duration, 'Success', end, jobid, "TRANSFORM", dry_run, scheduled_date)
                            
                        except Exception as e:
                            # ROLLBACK transaction
                            error_message = str(e)
                            error(f"Error executing transform for {sink}: {error_message}", dry_run=dry_run)
                            rollback_transaction(session, dry_run)
                            end = datetime.now()
                            duration = (end - start).total_seconds()
                            info(f"Duration in seconds: {duration}", dry_run=dry_run)
                            log_audit(session, None, -1, -1, -1, False, duration, error_message, end, jobid, "TRANSFORM", dry_run, scheduled_date)
                            raise e

                    kwargs.pop('params', None)
                    kwargs.pop('events', None)

                    return DAGTask(
                        name=task_id, 
                        definition=StoredProcedureCall(
                            func = fun,
                            args=[False, None], 
                            stage_location=self.stage_location,
                            imports=[(self.ai_zip, 'ai')],
                            packages=self.packages,
                        ), 
                        comment=comment, 
                        **kwargs
                    )
                else:
                    # sink statements are required
                    raise ValueError(f"Transform '{sink}' statements not found")
            elif task_type == TaskType.LOAD:
                json_context = self.caller_globals.get('json_context', None)
                if json_context:
                    import json
                    context = json.loads(json_context).get(sink, None)
                    if context:
                        sl_incoming_file_stage = self.sl_incoming_file_stage
                        if not sl_incoming_file_stage:
                            raise ValueError(f"sl_incoming_file_stage for {sink} not found")
                        table_name = context.get('tempTableName', None) or sink
                        variant = context.get('variant', "false")
                        context_schema: dict = context.get('schema', dict())
                        pattern: str = context_schema.get('pattern', None)
                        if not pattern:
                            raise ValueError(f"Pattern for {sink} not found")
                        metadata: dict = context_schema.get('metadata', dict())

                        from ai.starlake.helper import SnowflakeLoadTaskHelper
                        helper = SnowflakeLoadTaskHelper(sl_incoming_file_stage=sl_incoming_file_stage, pattern=pattern, table_name=table_name, metadata=metadata, variant=variant, sink=sink, domain=domain, table=table, audit=audit, expectations=expectations, expectation_items=expectation_items, name=self.pipeline_id, timezone=self.timezone)

                        info = helper.info
                        error = helper.error

                        begin_transaction = helper.begin_transaction
                        commit_transaction = helper.commit_transaction
                        rollback_transaction = helper.rollback_transaction

                        execute_sql = helper.execute_sql
                        execute_sqls = helper.execute_sqls

                        get_task_logical_date = helper.get_task_logical_date
                        as_datetime = helper.as_datetime
                        get_start_end_dates = helper.get_start_end_dates

                        check_if_dataset_exists = helper.check_if_dataset_exists
                        create_domain_if_not_exists = helper.create_domain_if_not_exists

                        log_audit = helper.log_audit
                        get_audit_info = helper.get_audit_info

                        run_expectations = helper.run_expectations

                        update_table_schema = helper.update_table_schema

                        build_copy = helper.build_copy

                        # create the function that will execute the load
                        def fun(session: Session, dry_run: bool, logical_date: Optional[str] = None) -> None:
                            # get the current job id
                            query = "SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID')::string"
                            rows = execute_sql(session, query, "Get the current task graph run group id", dry_run)
                            if rows.__len__() == 1:
                                jobid = rows[0][0]
                            else:
                                jobid = sink

                            backfill: bool = False
                            if allow_overlapping_execution:
                                query = "SELECT SYSTEM$TASK_RUNTIME_INFO('IS_BACKFILL')::boolean"
                                rows = execute_sql(session, query, "Check if the current running dag is a backfill", dry_run)
                                if rows.__len__() == 1:
                                    backfill = rows[0][0]

                            if not logical_date:
                                logical_date = get_task_logical_date(session, backfill, dry_run=dry_run)
                            logical_date = as_datetime(logical_date)

                            if cron_expr and not backfill:
                                # if a cron expression has been provided, the scheduled date corresponds to the end date determined by applying the cron expression to the logical date
                                (_, scheduled_date) = get_start_end_dates(cron_expr, logical_date)
                            else:
                                scheduled_date = logical_date

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
                                    exists = check_if_dataset_exists(session, f"{domain}.{table}")
                                    if exists:
                                        # enable change tracking
                                        # enable_change_tracking(session, sink, dry_run)
                                        # update table schema
                                        update_table_schema(session, schema_string=statements.get("schemaString", ""), sync_strategy="ADD", dry_run=dry_run)
                                    if write_strategy == 'WRITE_TRUNCATE':
                                        # truncate table
                                        execute_sql(session, f"TRUNCATE TABLE {sink}", "Truncate table", dry_run)
                                    # copy data
                                    copy_results = execute_sql(session, build_copy(), "Copy data", dry_run)
                                    if not exists:
                                        # enable change tracking
                                        # enable_change_tracking(session, sink, dry_run)
                                        ...
                                elif nbSteps == 2:
                                    # execute first step
                                    execute_sqls(session, statements.get('firstStep', []), "Execute first step", dry_run)
                                    if write_strategy == 'WRITE_TRUNCATE':
                                        # truncate table
                                        execute_sql(session, f"TRUNCATE TABLE {sink}", "Truncate table", dry_run)
                                    # copy data
                                    copy_results = execute_sql(session, build_copy(), "Copy data", dry_run)
                                    second_step = statements.get('secondStep', dict())
                                    # execute preActions
                                    execute_sqls(session, second_step.get('preActions', []), "Pre actions", dry_run)
                                    # execute schema presql
                                    execute_sqls(session, context_schema.get('presql', []), "Pre sqls", dry_run)
                                    if check_if_dataset_exists(session, f"{domain}.{table}"):
                                        # enable change tracking
                                        # enable_change_tracking(session, sink, dry_run)
                                        # execute addSCD2ColumnsSqls
                                        execute_sqls(session, second_step.get('addSCD2ColumnsSqls', []), "Add SCD2 columns", dry_run)
                                        # update schema
                                        update_table_schema(session, schema_string=statements.get("schemaString", ""), sync_strategy="ADD", dry_run=dry_run)
                                        # execute mainSqlIfExists
                                        execute_sqls(session, second_step.get('mainSqlIfExists', []), "Main sql if exists", dry_run)
                                    else:
                                        # execute mainSqlIfNotExists
                                        execute_sqls(session, second_step.get('mainSqlIfNotExists', []), "Main sql if not exists", dry_run)
                                        # enable change tracking
                                        # enable_change_tracking(session, sink, dry_run)
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
                                info(f"Duration in seconds: {duration}", dry_run=dry_run)
                                files, first_error_line, first_error_column_name, rows_parsed, rows_loaded, errors_seen = get_audit_info(copy_results, dry_run=dry_run)
                                message = first_error_line + '\n' + first_error_column_name
                                success = errors_seen == 0
                                log_audit(session, files, rows_parsed, rows_loaded, errors_seen, success, duration, message, end, jobid, "LOAD", dry_run, scheduled_date)
                                
                            except Exception as e:
                                # ROLLBACK transaction
                                error_message = str(e)
                                error(f"Error executing load for {sink}: {error_message}", dry_run=dry_run)
                                rollback_transaction(session, dry_run)
                                end = datetime.now()
                                duration = (end - start).total_seconds()
                                info(f"Duration in seconds: {duration}", dry_run=dry_run)
                                log_audit(session, None, -1, -1, -1, False, duration, error_message, end, jobid, "LOAD", dry_run, scheduled_date)
                                raise e

                        kwargs.pop('params', None)
                        kwargs.pop('events', None)

                        return DAGTask(
                            name=task_id, 
                            definition=StoredProcedureCall(
                                func = fun,
                                args=[False, None], 
                                stage_location=self.stage_location,
                                imports=[(self.ai_zip, 'ai')],
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
                raise NotImplementedError(f"{task_type} is not implemented")
        else:
            # sink is required
            raise ValueError("sink is required")
