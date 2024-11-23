from __future__ import annotations

from ai.starlake.job import IStarlakeJob, StarlakeSparkConfig

from ai.starlake.orchestration.starlake_schedules import StarlakeSchedules
from ai.starlake.orchestration.starlake_dependencies import StarlakeDependencies

import sys

from typing import Generic, List, TypeVar, Union

U = TypeVar("U")

T = TypeVar("T")

class IStarlakeOrchestration(Generic[U, T]):
    def __init__(self, filename: str, module_name: str, job: IStarlakeJob[T], **kwargs) -> None:
        """Generic Starlake orchestration class.
        Args:
            job (IStarlakeJob[T]): The job to use.
        """
        super().__init__(**kwargs) 
        self.job = job
        self.options = job.options
        self.caller_filename = filename

        # Get the name of the caller module
        self.caller_module_name = module_name
        
        # Access the caller's global variables
        self.caller_globals = sys.modules[self.caller_module_name].__dict__

        def default_spark_config(*args, **kwargs) -> StarlakeSparkConfig:
            return StarlakeSparkConfig(
                memory=self.caller_globals.get('spark_executor_memory', None),
                cores=self.caller_globals.get('spark_executor_cores', None),
                instances=self.caller_globals.get('spark_executor_instances', None),
                cls_options=job,
                options=self.options,
                **kwargs
            )

        self.spark_config: StarlakeSparkConfig = getattr(self.caller_module_name, "get_spark_config", default_spark_config)


    def sl_generate_scheduled_tables(self, schedules: StarlakeSchedules, **kwargs) -> Union[U, List[U]]:
        """Generate the Starlake dags that will orchestrate the load of the specified domains.

        Args:
            schedules (StarlakeSchedules): The required schedules
        
        Returns:
            Union[U, List[U]]: The generated dags, one for each schedule.
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
