from __future__ import annotations

from ai.starlake.job import IStarlakeJob, StarlakeSparkConfig, StarlakePreLoadStrategy

from ai.starlake.orchestration.starlake_schedules import StarlakeSchedules
from ai.starlake.orchestration.starlake_dependencies import StarlakeDependencies

import inspect

import sys

from typing import Generic, List, TypeVar, Union

T = TypeVar("T")

U = TypeVar("U")

class IStarlakeOrchestration(Generic[U], IStarlakeJob[T]):
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], options: dict=None, **kwargs) -> None:
        """Overrides IStarlakeJob.__init__()
        Args:
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The pre-load strategy to use.
            options (dict): The options to use.
        """
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs) 

        # Get the current call stack
        stack = inspect.stack()
        
        # Get the caller's stack frame
        caller_frame = stack[1]

        # Retrieve the filename of the caller (the one who instantiated this class)
        self.caller_filename = caller_frame.filename

        # Get the name of the caller module
        self.caller_module_name = caller_frame.frame.f_globals["__name__"]
        
        # Access the caller's global variables
        self.caller_globals = sys.modules[self.caller_module_name].__dict__

        def default_spark_config(*args, **kwargs) -> StarlakeSparkConfig:
            return StarlakeSparkConfig(
                memory=self.caller_globals.get('spark_executor_memory', None),
                cores=self.caller_globals.get('spark_executor_cores', None),
                instances=self.caller_globals.get('spark_executor_instances', None),
                cls_options=self,
                options=self.options,
                **kwargs
            )

        self.spark_config: StarlakeSparkConfig = getattr(self.caller_module_name, "get_spark_config", default_spark_config)

    def sl_generate_scheduled_tables(self, schedules: StarlakeSchedules, **kwargs) -> List[U]:
        """Generate the Starlake dags that will orchestrate the load of the specified domains.

        Args:
            schedules (StarlakeSchedules): The required schedules
        
        Returns:
            List[U]: The generated dags, one for each schedule.
        """

        pass

    def sl_generate_scheduled_tasks(self, dependencies: StarlakeDependencies, **kwargs) -> U:
        """Generate the Starlake dag that will orchestrate the specified tasks.

        Args:
            dependencies (StarlakeDependencies): The required dependencies
        
        Returns:
            U: The generated dag.
        """

        pass
