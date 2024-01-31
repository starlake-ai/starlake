from __future__ import annotations

from ai.starlake.job.starlake_pre_load_strategy import StarlakePreLoadStrategy
from ai.starlake.job.starlake_options import StarlakeOptions
from ai.starlake.job.spark_config import StarlakeSparkConfig

from typing import Generic, TypeVar, Union

T = TypeVar("T")

class IStarlakeJob(Generic[T], StarlakeOptions):
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], options: dict, **kwargs) -> None:
        """Init the class.
        Args:
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The pre-load strategy to use.
            options (dict): The options to use.
        """
        super().__init__(**kwargs)
        self.options = {} if not options else options
        pre_load_strategy = __class__.get_context_var(
            var_name="pre_load_strategy",
            default_value=StarlakePreLoadStrategy.NONE,
            options=self.options
        ) if not pre_load_strategy else pre_load_strategy

        if isinstance(pre_load_strategy, str):
            pre_load_strategy = \
                StarlakePreLoadStrategy(pre_load_strategy) if StarlakePreLoadStrategy.is_valid(pre_load_strategy) \
                    else StarlakePreLoadStrategy.NONE

        self.pre_load_strategy: StarlakePreLoadStrategy = pre_load_strategy

        self.sl_env_vars = __class__.get_sl_env_vars(self.options)
        self.sl_root = __class__.get_sl_root(self.options)
        self.sl_datasets = __class__.get_sl_datasets(self.options)

    def sl_import(self, task_id: str, domain: str, **kwargs) -> T:
        """Import job.
        Generate the scheduler task that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id.
            domain (str): The required domain to import.

        Returns:
            T: The scheduler task.
        """
        task_id = f"{domain}_import" if not task_id else task_id
        arguments = ["import", "--include", domain]
        return self.sl_job(task_id=task_id, arguments=arguments, **kwargs)

    def sl_pre_load(self, domain: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, **kwargs) -> Union[T, None]:
        """Pre-load job.
        Generate the scheduler task that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Union[T, None]: The scheduler task or None.
        """
        pass

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None, **kwargs) -> T:
        """Load job.
        Generate the scheduler task that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id.
            domain (str): The required domain of the table to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
        
        Returns:
            T: The scheduler task.
        """
        task_id = f"{domain}_{table}_load" if not task_id else task_id
        arguments = ["load", "--domains", domain, "--tables", table]
        return self.sl_job(task_id=task_id, arguments=arguments, spark_config=spark_config, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, spark_config: StarlakeSparkConfig=None, **kwargs) -> T:
        """Transform job.
        Generate the scheduler task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id.
            transform_name (str): The transform to run.
            transform_options (str): The optional transform options to use.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
        
        Returns:
            T: The scheduler task.
        """
        task_id = f"{transform_name}" if not task_id else task_id
        arguments = ["transform", "--name", transform_name]
        transform_options = transform_options if transform_options else self.__class__.get_context_var(transform_name, {}, self.options).get("options", "")
        if transform_options:
            arguments.extend(["--options", transform_options])
        return self.sl_job(task_id=task_id, arguments=arguments, spark_config=spark_config, **kwargs)

    def pre_tasks(self, *args, **kwargs) -> Union[T, None]:
        """Pre tasks."""
        return None

    def post_tasks(self, *args, **kwargs) -> Union[T, None]:
        """Post tasks."""
        return None

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> T:
        """Generic job.
        Generate the scheduler task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
        
        Returns:
            T: The scheduler task.
        """
        pass

