from typing import List, Optional, Union

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, StarlakeExecutionEnvironment, TaskType

from ai.starlake.dataset import StarlakeDataset, AbstractEvent
from ai.starlake.odbc import SQLTask, SQLEmptyTask, SQLTaskFactory

class SQLEvent(AbstractEvent[StarlakeDataset]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> StarlakeDataset:
        return dataset

class StarlakeSQLJob(IStarlakeJob[SQLTask, StarlakeDataset], StarlakeOptions, SQLEvent):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        timezone = kwargs.get('timezone', __class__.get_context_var(var_name='timezone', default_value='UTC', options=self.options))
        self._timezone = timezone

    @property
    def timezone(self) -> str:
        return self._timezone

    @classmethod
    def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str]:
        """Returns the orchestrator to use.
        Returns:
            StarlakeOrchestrator: The orchestrator to use.
        """
        return StarlakeOrchestrator.STARLAKE

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.SQL

    def dummy_op(self, task_id: str, events: Optional[List[StarlakeDataset]] = None, task_type: Optional[TaskType] = TaskType.EMPTY, **kwargs) -> SQLTask:
        """Dummy op.
        Generate a SQL dummy task.

        Args:
            task_id (str): The required task id.
            events (Optional[List[StarlakeDataset]]): The optional events to materialize.
            task_type (Optional[TaskType]): The optional task type.

        Returns:
            SQLTask: The SQL task.
        """
        comment = kwargs.get('comment', None)
        if not comment:
            comment = f"dummy task for {task_id}"
        kwargs.pop('comment', None)

        return SQLEmptyTask(
            sink=task_id,
            caller_globals=self.caller_globals,            
            comment=comment, 
            task_type=task_type,
            **kwargs
        )

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: Optional[StarlakeSparkConfig]=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> SQLTask:
        """Overrides IStarlakeJob.sl_load()
        Generate the SQL task that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id.
            domain (str): The required domain of the table to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Union[StarlakeDataset, str]): The optional dataset to materialize.

        Returns:
            SQLTask: The SQL task.
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

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> SQLTask:
        """Overrides IStarlakeJob.sl_transform()
        Generate the SQL task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            SQLTask: The SQL task.
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

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType]=None, **kwargs) -> SQLTask:
        """Overrides IStarlakeJob.sl_job()
        Generate the SQL task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            SQLTask: The SQL task.
        """
        sink = kwargs.get('sink', None)
        if not sink and dataset:
            if isinstance(dataset, str):
                sink = dataset
            else:
                sink = dataset.sink
        kwargs.pop('sink', None)
        return SQLTaskFactory.task(caller_globals=self.caller_globals, sink=sink, arguments=arguments, options=self.options, task_type=task_type, **kwargs)
