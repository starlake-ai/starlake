from __future__ import annotations

from abc import abstractmethod

from ai.starlake.common import MissingEnvironmentVariable, StarlakeCronPeriod, sl_schedule_format

from ai.starlake.job.starlake_pre_load_strategy import StarlakePreLoadStrategy
from ai.starlake.job.starlake_options import StarlakeOptions
from ai.starlake.job.spark_config import StarlakeSparkConfig

from ai.starlake.dataset import AbstractEvent, StarlakeDataset, DatasetTriggeringStrategy

import importlib

import inspect

import os

import sys

from datetime import datetime, timedelta
import pytz

from typing import final, Generic, List, Optional, Tuple, Type, TypeVar, Union

T = TypeVar("T")

E = TypeVar("E")

from enum import Enum

class StarlakeOrchestrator(str, Enum):
    AIRFLOW = "airflow"
    COMPOSER = "airflow"
    DAGSTER = "dagster"
    SNOWFLAKE = "snowflake"
    STARLAKE = "starlake"

    def __str__(self):
        return self.value

class StarlakeExecutionEnvironment(str, Enum):

    CLOUD_RUN = "cloud_run"
    DATAPROC = "dataproc"
    FARGATE = "fargate"
    SHELL = "shell"
    SQL = "sql"

    def __str__(self):
        return self.value

class StarlakeExecutionMode(str, Enum):
    
    DRY_RUN = "dry_run"
    RUN = "run"
    BACKFILL = "backfill"

    def __str__(self):
        return self.value

class TaskType(str, Enum):
    START = "start"
    PRELOAD = "preload"
    IMPORT = "import" # Deprecated, use STAGE instead
    STAGE = "stage"
    LOAD = "load"
    TRANSFORM = "transform"
    EMPTY = "empty"
    END = "end"

    def __str__(self):
        return self.value

    @classmethod
    def from_str(cls, value: str) -> Optional["TaskType"]:
        """Returns an instance of TaskType if the value is valid, otherwise None."""
        try:
            return cls(value.lower())
        except ValueError:
            return None

class IStarlakeJob(Generic[T, E], StarlakeOptions, AbstractEvent[E]):
    def __init__(self, filename: Optional[str] = None, module_name: Optional[str] = None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, options: dict = {}, **kwargs) -> None:
        """Init the class.
        Args:
            filename (str): The filename from which the job is called.
            module_name (str): The module name from which the job is called.
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
        self.sl_schedule_parameter_name = __class__.get_context_var(
            var_name="sl_schedule_parameter_name",
            default_value="sl_schedule",
            options=self.options
        )
        self.sl_schedule_format = __class__.get_context_var(
            var_name="sl_schedule_format",
            default_value=sl_schedule_format,
            options=self.options
        )
        try:
            self.retries = int(__class__.get_context_var(var_name='retries', options=self.options))
        except (MissingEnvironmentVariable, ValueError):
            self.retries = 1
        try:
            self.retry_delay = int(__class__.get_context_var(var_name='retry_delay', options=self.options))
        except (MissingEnvironmentVariable, ValueError):
            self.retry_delay = 300

        # Define the source
        self.source = filename.replace(".py", "").replace(".pyc", "").lower() if filename else None

        # Access the caller file name
        self.caller_filename = filename

        # Access the caller module name
        self.caller_module_name = module_name
        
        # Access the caller's global variables
        import sys
        self.caller_globals = sys.modules[self.caller_module_name].__dict__ if module_name else {}

        def default_spark_config(*args, **kwargs) -> StarlakeSparkConfig:
            return StarlakeSparkConfig(
                memory=self.caller_globals.get('spark_executor_memory', None),
                cores=self.caller_globals.get('spark_executor_cores', None),
                instances=self.caller_globals.get('spark_executor_instances', None),
                cls_options=self,
                options=self.options,
                **kwargs
            )

        self.get_spark_config = getattr(self.caller_module_name, "get_spark_config", default_spark_config) if module_name else default_spark_config

        self._events: List[E] = []

        self._cron_period_frequency = StarlakeCronPeriod.from_str(__class__.get_context_var('cron_period_frequency', default_value='week', options=self.options))

        default_dataset_triggering_strategy = DatasetTriggeringStrategy.ANY
        dataset_triggering_strategy = __class__.get_context_var(
            var_name="dataset_triggering_strategy",
            default_value=default_dataset_triggering_strategy,
            options=self.options
        )
        if isinstance(dataset_triggering_strategy, str):
            dataset_triggering_strategy = \
                DatasetTriggeringStrategy(dataset_triggering_strategy) if DatasetTriggeringStrategy.is_valid(dataset_triggering_strategy) \
                    else default_dataset_triggering_strategy

        self.__dataset_triggering_strategy: DatasetTriggeringStrategy = dataset_triggering_strategy

        self.__timezone = kwargs.get('timezone', __class__.get_context_var(var_name='timezone', default_value='UTC', options=self.options))
        # set start_date
        module = sys.modules.get(module_name) if module_name else None
        if module and hasattr(module, '__file__'):
            import os
            file_path = module.__file__
            stat = os.stat(file_path)
            default_start_date = datetime.fromtimestamp(stat.st_mtime, tz=pytz.timezone(self.timezone)).strftime('%Y-%m-%d')
        else:
            default_start_date = datetime.now().astimezone(pytz.timezone(self.timezone)).strftime('%Y-%m-%d')
        sd = __class__.get_context_var(var_name='start_date', default_value=default_start_date, options=self.options)
        import re
        pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        if pattern.fullmatch(sd):
            self.__start_date = datetime.strptime(sd, '%Y-%m-%d').astimezone(pytz.timezone(self.timezone))
        else:
            self.__start_date = datetime.strptime(default_start_date, '%Y-%m-%d').astimezone(pytz.timezone(self.timezone))

        self.__optional_dataset_enabled = str(__class__.get_context_var(var_name='optional_dataset_enabled', default_value="false", options=self.options)).strip().lower() == "true"
        self.__data_cycle_enabled = str(__class__.get_context_var(var_name='data_cycle_enabled', default_value="false", options=self.options)).strip().lower() == "true"
        self.__data_cycle = str(__class__.get_context_var(var_name='data_cycle', default_value="none", options=self.options))
        self.__beyond_data_cycle_enabled = str(__class__.get_context_var(var_name='beyond_data_cycle_enabled', default_value="true", options=self.options)).strip().lower() == "true"
        self.__min_timedelta_between_runs = int(__class__.get_context_var(var_name='min_timedelta_between_runs', default_value=15*60, options=self.options))
        self.__run_dependencies_first = __class__.get_context_var(var_name='run_dependencies_first', default_value='False', options=self.options).lower() == 'true'
        self.__pipeline_id = self.caller_filename.replace(".py", "").replace(".pyc", "").upper()

    @property
    def dataset_triggering_strategy(self) -> DatasetTriggeringStrategy:
        return self.__dataset_triggering_strategy

    @property
    def timezone(self) -> str:
        return self.__timezone

    @property
    def start_date(self) -> datetime:
        """Get the start date of the job"""
        return self.__start_date

    @property
    def optional_dataset_enabled(self) -> bool:
        """whether a dataset can be optional or not."""
        return self.__optional_dataset_enabled

    @property
    def data_cycle_enabled(self) -> bool:
        """Get whether data cycle is enabled or not"""
        return self.__data_cycle_enabled

    @property
    def data_cycle(self) -> str:
        """Get the data cycle of the job"""
        return self.__data_cycle

    @data_cycle.setter
    def data_cycle(self, value: Optional[str]) -> None:
        """Set the data cycle value."""
        if self.data_cycle_enabled and value:
            data_cycle = value.strip().lower()
            if data_cycle == "none":
                self.__data_cycle = None
            elif data_cycle == "hourly":
                self.__data_cycle = "0 * * * *"
            elif data_cycle == "daily":
                self.__data_cycle = "0 0 * * *"
            elif data_cycle == "weekly":
                self.__data_cycle = "0 0 * * 0"
            elif data_cycle == "monthly":
                self.__data_cycle = "0 0 1 * *"
            elif data_cycle == "yearly":
                self.__data_cycle = "0 0 1 1 *"
            elif is_valid_cron(data_cycle):
                self.__data_cycle = data_cycle
            else:
                raise ValueError(f"Invalid data cycle value: {data_cycle}")
        else:
            self.__data_cycle = None

    @property
    def beyond_data_cycle_enabled(self) -> bool:
        """whether the beyond data cycle feature is enabled or not."""
        return self.__beyond_data_cycle_enabled

    @property
    def min_timedelta_between_runs(self) -> int:
        """Get minimum time delta in seconds between two consecutive runs"""
        return self.__min_timedelta_between_runs

    @property
    def run_dependencies_first(self) -> bool:
        """whether to run dependencies first or not."""
        return self.__run_dependencies_first

    @property
    def pipeline_id(self) -> str:
        """Get the pipeline id."""
        return self.__pipeline_id

    @classmethod
    def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str, None]:
        """Returns the orchestrator to use.

        Returns:
            StarlakeOrchestrator: The orchestrator to use.
        """
        return None

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str, None]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return None

    @property
    def events(self) -> List[E]:
        """Returns the events.

        Returns:
            List[E]: The events.
        """
        return self._events

    @events.setter
    def events(self, events: List[E]):
        """Sets the events.

        Args:
            events (List[E]): The events.
        """
        self._events = events

    @final
    def __add_event(self, dataset: Union[str, StarlakeDataset], **kwargs) -> E:
        if isinstance(dataset, str):
            dataset = StarlakeDataset(name=dataset, **kwargs)
        event = self.to_event(dataset, source=kwargs.get('source', self.source))
        events = self.events
        events.append(event)
        self.events = events
        return event

    def sl_dataset(self, uri: str, **kwargs) -> str:
        """Returns the dataset from the specified uri.

        Args:
            uri (str): The uri of the dataset.

        Returns:
            str: The dataset.
        """

        from ai.starlake.common import sanitize_id, asQueryParameters, sl_schedule
        cron = kwargs.get('cron', kwargs.get('params', dict()).get('cron', None))
        parameters: dict = dict()
        if cron is not None :
            parameters[self.sl_schedule_parameter_name] = sl_schedule(cron, format=self.sl_schedule_format)

        return sanitize_id(uri).lower() + asQueryParameters(parameters)

    def sl_dataset_url(self, dataset: StarlakeDataset, **kwargs) -> str:
        return dataset.url

    def sl_import(self, task_id: str, domain: str, tables: set=set(), **kwargs) -> T:
        """Import job.
        Generate the scheduler task that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id.
            domain (str): The required domain to import.
            tables (set): The optional tables to import.

        Returns:
            T: The scheduler task.
        """
        params = kwargs.get("params", {})
        schedule = params.get('schedule', None)
        if schedule is not None:
            tmp_domain = f'{domain}_{schedule}'
        else:
            tmp_domain = domain
        self.__add_event(tmp_domain, **kwargs)
        task_id = f"import_{tmp_domain}" if not task_id else task_id
        kwargs.pop("task_id", None)
        arguments = [TaskType.STAGE.value, "--domains", domain, "--tables", ",".join(tables), "--options", "SL_RUN_MODE=main,SL_LOG_LEVEL=info"]
        return self.sl_job(task_id=task_id, arguments=arguments, task_type=TaskType.STAGE, **kwargs)

    @classmethod
    def get_sl_pre_load_task_id(cls, domain: str, pre_load_strategy: StarlakePreLoadStrategy, **kwargs) -> Optional[str]:
        if pre_load_strategy == StarlakePreLoadStrategy.NONE:
            return None
        else:
            from ai.starlake.common import sanitize_id

            orchestrator = cls.sl_orchestrator()

            if orchestrator == StarlakeOrchestrator.DAGSTER:
                params = kwargs.get("params", {})
                schedule = params.get('schedule', None)
                if schedule is not None:
                    domain = f'{domain}_{schedule}'

            if pre_load_strategy == StarlakePreLoadStrategy.IMPORTED:
                return sanitize_id(f'check_{domain}_incoming_files')

            elif pre_load_strategy == StarlakePreLoadStrategy.PENDING:
                return sanitize_id(f'check_{domain}_pending_files')

            elif pre_load_strategy == StarlakePreLoadStrategy.ACK:
                return sanitize_id(f'check_{domain}_ack_file')


    def sl_pre_load(self, domain: str, tables: set=set(), pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, **kwargs) -> Optional[T]:
        """Pre-load job.
        Generate the scheduler task that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to pre-load.
            tables (set): The optional tables to pre-load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Optional[T]: The scheduler task or None.
        """
        if isinstance(pre_load_strategy, str):
            pre_load_strategy = \
                StarlakePreLoadStrategy(pre_load_strategy) if StarlakePreLoadStrategy.is_valid(pre_load_strategy) \
                    else self.pre_load_strategy

        pre_load_strategy = self.pre_load_strategy if not pre_load_strategy else pre_load_strategy

        if pre_load_strategy == StarlakePreLoadStrategy.NONE:
            return None
        else:
            arguments = [TaskType.PRELOAD.value, "--domain", domain, "--tables", ",".join(tables), "--strategy", pre_load_strategy.value, "--options", "SL_RUN_MODE=main,SL_LOG_LEVEL=info"]

            task_id = kwargs.get('task_id', __class__.get_sl_pre_load_task_id(domain, pre_load_strategy, **kwargs))

            kwargs.pop("task_id", None)
            
            if pre_load_strategy == StarlakePreLoadStrategy.ACK:

                def current_dt():
                    from datetime import datetime
                    return datetime.today().strftime('%Y-%m-%d')

                ack_file = kwargs.get(
                    'ack_file', 
                    __class__.get_context_var(
                        var_name='global_ack_file_path',
                        default_value=f'{self.sl_datasets}/pending/{domain}/{current_dt()}.ack',
                        options=self.options
                    )
                )
                kwargs.pop("ack_file", None)

                arguments.extend(["--globalAckFilePath", f"{ack_file}"])

                ack_wait_timeout = int(
                    kwargs.get(
                        'ack_wait_timeout',
                            __class__.get_context_var(
                            var_name='ack_wait_timeout',
                            default_value=60*60, # 1 hour
                            options=self.options
                        )
                    )
                )
                kwargs.pop("ack_wait_timeout", None)

                kwargs.update({'retry_delay': timedelta(seconds=ack_wait_timeout)})

            return self.sl_job(task_id=task_id, arguments=arguments, task_type=TaskType.PRELOAD, **kwargs)

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: Optional[StarlakeSparkConfig]=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> T:
        """Load job.
        Generate the scheduler task that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id.
            domain (str): The required domain of the table to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Union[StarlakeDataset, str]): The optional dataset to materialize.
        
        Returns:
            T: The scheduler task.
        """
        task_id = kwargs.get("task_id", f"load_{domain}_{table}") if not task_id else task_id
        kwargs.pop("task_id", None)
        if not dataset:
            params: dict = kwargs.get('params', dict())
            params.update({
                'sl_schedule_parameter_name': self.sl_schedule_parameter_name, 
                'sl_schedule_format': self.sl_schedule_format
            })
            kwargs['params'] = params
            dataset = StarlakeDataset(name=f'{domain}.{table}', **kwargs)
        self.__add_event(dataset, **kwargs)
        arguments = [TaskType.LOAD.value, "--domains", domain, "--tables", table]
        if spark_config is None:
            spark_config = self.get_spark_config(
                self.__class__.get_context_var(
                    'spark_config_name', 
                    f'{domain}.{table}'.lower(),
                    options=self.options
                ), 
                **self.caller_globals.get('spark_properties', {})
            )
        return self.sl_job(task_id=task_id, arguments=arguments, spark_config=spark_config, dataset=dataset, task_type=TaskType.LOAD, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, spark_config: Optional[StarlakeSparkConfig]=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> T:
        """Transform job.
        Generate the scheduler task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id.
            transform_name (str): The transform to run.
            transform_options (str): The optional transform options to use.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Union[StarlakeDataset, str]): The optional dataset to materialize.
        
        Returns:
            T: The scheduler task.
        """
        task_id = kwargs.get("task_id", f"{transform_name}") if not task_id else task_id
        kwargs.pop("task_id", None)
        if not dataset:
            params: dict = kwargs.get('params', dict())
            params.update({
                'sl_schedule_parameter_name': self.sl_schedule_parameter_name, 
                'sl_schedule_format': self.sl_schedule_format
            })
            kwargs['params'] = params
            dataset = StarlakeDataset(name=transform_name, **kwargs)
        self.__add_event(dataset, **kwargs)
        arguments = [TaskType.TRANSFORM.value, "--name", transform_name]
        options = list()
        if transform_options:
            options = transform_options.split(",")
        additional_options = self.__class__.get_context_var(transform_name, {}, self.options).get("options", "")
        if additional_options.__len__() > 0:
            options.extend(additional_options.split(","))
        if options.__len__() > 0:
            arguments.extend(["--options", ",".join(options)])
        if spark_config is None:
            spark_config = self.get_spark_config(
                self.__class__.get_context_var(
                    'spark_config_name', 
                    transform_name.lower(),
                    options=self.options
                ), 
                **self.caller_globals.get('spark_properties', {})
            )
        return self.sl_job(task_id=task_id, arguments=arguments, spark_config=spark_config, dataset=dataset, task_type=TaskType.TRANSFORM, **kwargs)

    def pre_tasks(self, *args, **kwargs) -> Optional[T]: #TODO rename to pre_ops
        """Pre tasks."""
        return None

    def post_tasks(self, *args, **kwargs) -> Optional[T]: #TODO rename to post_ops
        """Post tasks."""
        return None

    def start_op(self, task_id: str, scheduled: bool, not_scheduled_datasets: Optional[List[StarlakeDataset]], least_frequent_datasets: Optional[List[StarlakeDataset]], most_frequent_datasets: Optional[List[StarlakeDataset]], **kwargs) -> Optional[T]:
        """Start operation."""
        events = kwargs.get('events', [])
        kwargs.pop('events', None)
        if not scheduled and least_frequent_datasets:
            datasets = least_frequent_datasets
        else:
            datasets = None
        return self.dummy_op(task_id, list(map(lambda dataset: self.to_event(dataset=dataset, source=self.source), datasets or [])), task_type=TaskType.START, **kwargs)

    def end_op(self, task_id: str, events: Optional[List[E]] = None, **kwargs) -> Optional[T]:
        """End operation."""
        return self.dummy_op(task_id, events, task_type=TaskType.END, **kwargs)

    @abstractmethod
    def dummy_op(self, task_id, events: Optional[List[E]], task_type: Optional[TaskType]=TaskType.EMPTY, **kwargs) -> T: 
        pass

    @abstractmethod
    def skip_or_start_op(self, task_id: str, upstream_task: T, **kwargs) -> Optional[T]:
        return None

    @abstractmethod
    def sl_job(self, task_id: str, arguments: list, spark_config: Optional[StarlakeSparkConfig]=None, dataset: Optional[Union[StarlakeDataset, str]]=None, task_type: Optional[TaskType]=None, **kwargs) -> T:
        """Generic job.
        Generate the scheduler task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Union[StarlakeDataset, str]): The optional dataset to publish.
            task_type (TaskType): The optional task type.
        
        Returns:
            T: The scheduler task.
        """
        pass

    @final
    def sl_env(self, args: Union[str, List[str], None] = None) -> dict:
        """Returns the environment variables to use.

        Args:
            args(str | List[str] | None): The optional arguments to use. Defaults to None.

        Returns:
            dict: The environment variables.
        """
        import os
        env = os.environ.copy() # Copy the current environment variables

        if args is None:
            return env.update(self.sl_env_vars) # Add/overwrite with sl env variables
        elif isinstance(args, str):
            arguments = args.split(" ")
        else:
            arguments = args

        found = False

        for index, arg in enumerate(arguments):
            if arg == "--options" and arguments.__len__() > index + 1:
                opts = arguments[index+1]
                if opts.strip().__len__() > 0:
                    temp = self.sl_env_vars.copy() # Copy the current sl env variables
                    temp.update({
                        key: value
                        for opt in opts.split(",")
                        if "=" in opt  # Only process valid key=value pairs
                        for key, value in [opt.split("=")]
                    })
                    env.update(temp)
                else:
                    env.update(self.sl_env_vars) # Add/overwrite with sl env variables
                found = True
                break

        if not found:
            env.update(self.sl_env_vars) # Add/overwrite with sl env variables
        return env

    @property
    def cron_period_frequency(self) -> StarlakeCronPeriod:
        """Returns the cron period frequency.

        Returns:
            StarlakeCronPeriod: The cron period frequency.
        """
        return self._cron_period_frequency

class StarlakeJobFactory:
    _registry = {}

    _initialized = False

    @classmethod
    def register_jobs_from_package(cls, package_name: str = "ai.starlake") -> None:
        """
        Dynamically load all classes implementing IStarlakeJob from the given root package, including sub-packages,
        and register them in the StarlakeJobRegistry.
        """
        print(f"Registering jobs from package {package_name}")
        package = importlib.import_module(package_name)
        package_path = os.path.dirname(package.__file__)

        for root, dirs, files in os.walk(package_path):
            # Convert the filesystem path back to a Python module path
            relative_path = os.path.relpath(root, package_path)
            if relative_path == ".":
                module_prefix = package_name
            else:
                module_prefix = f"{package_name}.{relative_path.replace(os.path.sep, '.')}"

            for file in files:
                if file.endswith(".py") and file != "__init__.py":
                    module_name = os.path.splitext(file)[0]
                    full_module_name = f"{module_prefix}.{module_name}"

                    try:
                        module = importlib.import_module(full_module_name)
                    except ImportError as e:
                        print(f"Failed to import module {full_module_name}: {e}")
                        continue
                    except AttributeError as e:
                        print(f"Failed to import module {full_module_name}: {e}")
                        continue

                    for name, obj in inspect.getmembers(module, inspect.isclass):
                        if issubclass(obj, IStarlakeJob) and obj is not IStarlakeJob:
                            StarlakeJobFactory.register_job(obj)


    @classmethod
    def register_job(cls, job_class: Type[IStarlakeJob]) -> None:
        orchestrator = job_class.sl_orchestrator()
        if orchestrator is None:
            return
        execution_environment = job_class.sl_execution_environment()
        if execution_environment is None:
            return
        executions = cls._registry.get(orchestrator, {})
        executions.update({execution_environment: job_class})
        cls._registry.update({orchestrator: executions})
        print(f"Registered job {job_class} for orchestrator {orchestrator} and execution environment {execution_environment}")

    @classmethod
    def create_job(cls, filename: str, module_name: str, orchestrator: Union[StarlakeOrchestrator, str], execution_environment: Union[StarlakeExecutionEnvironment, str], options: dict, **kwargs) -> IStarlakeJob:
        if not cls._initialized:
            cls.register_jobs_from_package()
            cls._initialized = True
        executions: dict = cls._registry.get(orchestrator, {})
        job: Type[IStarlakeJob] = executions.get(execution_environment, None)
        if job is None:
            raise ValueError(f"Execution environment {execution_environment} for orchestrator {orchestrator} not found in registry")
        return job(filename=filename, module_name=module_name, options=options, **kwargs)
