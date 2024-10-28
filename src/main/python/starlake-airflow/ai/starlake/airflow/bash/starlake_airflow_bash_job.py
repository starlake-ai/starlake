from typing import Union

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig

from ai.starlake.airflow import StarlakeAirflowJob

from airflow.models.baseoperator import BaseOperator

from airflow.operators.bash import BashOperator

class StarlakeAirflowBashJob(StarlakeAirflowJob):
    """Airflow Starlake Bash Job."""
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs):
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
        """Overrides StarlakeAirflowJob.sl_job()
        Generate the Airflow task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.

        Returns:
            BaseOperator: The Airflow task.
        """
        found = False

        for index, arg in enumerate(arguments):
            if arg == "--options" and arguments.__len__() > index + 1:
                opts = arguments[index+1]
                if opts.__len__() > 0:
                    options = opts + "," + ",".join([f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())])
                else:
                    options = ",".join([f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())])
                arguments[index+1] = options
                found = True
                break

        if not found:
            arguments.append("--options")
            arguments.append(",".join([f"{key}={value}" for key, value in self.sl_env_vars.items()]))

        command = __class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options) + f" {' '.join(arguments)}"
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return BashOperator(
            task_id=task_id,
            bash_command=command,
            cwd=self.sl_root,
            **kwargs
        )
