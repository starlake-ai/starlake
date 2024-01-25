from typing import Union

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig

from ai.starlake.airflow import AirflowStarlakeJob

from airflow.models.baseoperator import BaseOperator

from airflow.operators.bash import BashOperator

class AirflowStarlakeBashJob(AirflowStarlakeJob):
    """Airflow Starlake Bash Job."""
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs):
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
        """Overrides AirflowStarlakeJob.sl_job()"""
        command = __class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options) + f" {' '.join(arguments)}"
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return BashOperator(
            task_id=task_id,
            bash_command=command,
            cwd=self.sl_root,
            **kwargs
        )
