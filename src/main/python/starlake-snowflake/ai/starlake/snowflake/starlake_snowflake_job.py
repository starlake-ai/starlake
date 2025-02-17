from typing import List, Optional, Tuple, Union

from functools import partial

from ai.starlake.common import MissingEnvironmentVariable

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, StarlakeExecutionEnvironment

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from snowflake.core.task import StoredProcedureCall
from snowflake.core.task.dagv1 import DAGTask

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.row import Row

class SnowflakeEvent(AbstractEvent[str]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> str:
        return dataset.refresh().url

class StarlakeSnowflakeJob(IStarlakeJob[DAGTask, str], StarlakeOptions, SnowflakeEvent):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        try:
            self._stage_location = kwargs.get('stage_location', __class__.get_context_var(var_name='stage_location', options=self.options))
        except MissingEnvironmentVariable:
            self._stage_location = None
        try:
            self._warehouse = kwargs.get('warehouse', __class__.get_context_var(var_name='warehouse', options=self.options))
        except MissingEnvironmentVariable:
            self._warehouse = None
        self._packages=["croniter", "sqlalchemy"]

    @property
    def stage_location(self) -> Optional[str]:
        return self._stage_location

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    @property
    def packages(self) -> List[str]:
        return self._packages

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

    def update_events(self, event: str, **kwargs) -> Tuple[(str, List[str])]:
        """Add the event to the list of Snowflake events that will be triggered.

        Args:
            event (str): The event to add.

        Returns:
            Tuple[(str, List[str]): The tuple containing the list of snowflake events to trigger.
        """
        events: List[str] = kwargs.get('events', [])
        events.append(event)
        return 'events', events

    def dummy_op(self, task_id: str, events: Optional[List[str]] = None, **kwargs) -> DAGTask:
        """Dummy op.
        Generate a Snowflake dummy task.
        If it represents the first task of a pipeline, it will define the optional streams that may trigger the DAG.

        Args:
            task_id (str): The required task id.
            events (Optional[List[str]]): The optional events to materialize.

        Returns:
            DAGTask: The Snowflake task.
        """
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"dummy task for {task_id}"
        kwargs.pop('comment', None)
        return DAGTask(name=task_id, definition=f"select '{task_id}'", comment=comment, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, sink: Optional[str] = None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Snowflake task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            sink (Optional[str], optional): The optional sink to write the transformed data.

        Returns:
            DAGTask: The Snowflake task.
        """
        kwargs.update({'transform': True})
        kwargs.update({'transform_name': transform_name})
        if sink:
            kwargs.update({'sink': sink})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> DAGTask:
        """Overrides IStarlakeJob.sl_job()
        Generate the Snowflake task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.

        Returns:
            DAGTask: The Snowflake task.
        """
        if kwargs.get('transform', False):
            kwargs.pop('transform', None)
            transform_name = kwargs.get('transform_name', None)
            if transform_name:
                kwargs.pop('transform', None)
                statements = self.caller_globals.get('statements', dict()).get(transform_name, None)
                if statements:
                    comment = kwargs.get('comment', f'Starlake {transform_name} task')
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

                    sink = kwargs.get('sink', None)
                    if not sink:
                        raise ValueError("sink is required")
                    else:
                        kwargs.pop('sink', None)

                    cron_expr = kwargs.get('cron_expr', None)
                    kwargs.pop('cron_expr', None)

                    # create the function that will execute the transform
                    if statements:
                        def fun(session: Session, sink: str, statements: dict, params: dict, cron_expr: Optional[str]) -> None:
                            from sqlalchemy import text

                            if cron_expr:
                                from croniter import croniter
                                from croniter.croniter import CroniterBadCronError
                                from datetime import datetime
                                try:
                                    croniter(cron_expr)
                                    start_time = datetime.fromtimestamp(datetime.now().timestamp())
                                    iter = croniter(cron_expr, start_time)
                                    curr = iter.get_current(datetime)
                                    previous = iter.get_prev(datetime)
                                    next = croniter(cron_expr, previous).get_next(datetime)
                                    if curr == next :
                                        sl_end_date = curr
                                    else:
                                        sl_end_date = previous
                                    sl_start_date = croniter(cron_expr, sl_end_date).get_prev(datetime)
                                    format = '%Y-%m-%d %H:%M:%S%z'
                                    params.update({'sl_start_date': sl_start_date.strftime(format), 'sl_end_date': sl_end_date.strftime(format)})
                                except CroniterBadCronError:
                                    raise ValueError(f"Invalid cron expression: {cron_expr}")

                            schemaAndTable = sink.split('.')
                            schema = schemaAndTable[0]
                            table = schemaAndTable[1]

                            # create SQL schema
                            session.sql(query=f"CREATE SCHEMA IF NOT EXISTS {schema}").collect() # use templating

                            # execute preActions
                            preActions: List[str] = statements.get('preActions', [])
                            for action in preActions:
                                stmt: str = text(action).bindparams(**params)
                                session.sql(stmt).collect()

                            # execute preSqls
                            preSqls: List[str] = statements.get('preSqls', [])
                            for sql in preSqls:
                                stmt: str = text(sql).bindparams(**params)
                                session.sql(stmt).collect()

                            # check if the sink exists
                            df: DataFrame = session.sql(query=f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'
  AND TABLE_NAME = '{table}'") # use templating
                            rows: List[Row] = df.collect()
                            if rows.__len__() > 0:
                                # execute mainSqlIfExists
                                sqls: List[str] = statements.get('mainSqlIfExists', [])
                                for sql in sqls:
                                    stmt: str = text(sql).bindparams(**params)
                                    session.sql(stmt).collect()
                            else:
                                # execute mainSqlIfNotExists
                                sqls: List[str] = statements.get('mainSqlIfNotExists', [])
                                for sql in sqls:
                                    stmt: str = text(sql).bindparams(**params)
                                    session.sql(stmt).collect()

                            # execute postSqls
                            postSqls: List[str] = statements.get('postSqls', [])
                            for sql in postSqls:
                                stmt: str = text(sql).bindparams(**params)
                                session.sql(stmt).collect()

                        return DAGTask(
                            name=task_id, 
                            definition=StoredProcedureCall(
                                func = partial(
                                    fun, 
                                    sink=sink, 
                                    statements=statements, 
                                    params=options, 
                                    cron_expr=cron_expr
                                ), 
                                stage_location=self.stage_location,
                                packages=self.packages
                            ), 
                            comment=comment, 
                            **kwargs
                        )
                else:
                    raise ValueError(f"Transform '{transform_name}' statements not found")
            else:
                # transform_name is required
                raise ValueError("transform_name is required")
        else:
            # transform is required
            raise NotImplementedError("Not implemented")
