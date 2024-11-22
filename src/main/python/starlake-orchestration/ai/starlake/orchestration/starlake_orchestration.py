from __future__ import annotations

from ai.starlake.job import IStarlakeJob, StarlakeSparkConfig, StarlakeOptions
from ai.starlake.orchestration.starlake_schedules import StarlakeSchedules
from ai.starlake.orchestration.starlake_dependencies import StarlakeDependencies

from typing import Generic, List, TypeVar, Union

T = TypeVar("T")

U = TypeVar("U")

class IStarlakeOrchestration(Generic[U], IStarlakeJob[T], StarlakeOptions):
    def __init__(self, job: IStarlakeJob[T], sparkConfig: StarlakeSparkConfig, **kwargs) -> None:
        """Initializes a new IStarlakeOrchestration instance.
            args:
                job (IStarlakeJob[T]): The required starlake job.
                sparkConfig (StarlakeSparkConfig): The required spark config.
        """
        self.job = job
        self.sparkConfig = sparkConfig
        self.options = job.options

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
