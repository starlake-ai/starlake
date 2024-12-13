from typing import Union

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment

from ai.starlake.airflow import StarlakeAirflowJob

from airflow.models.baseoperator import BaseOperator

from airflow.operators.bash import BashOperator

from airflow.operators.python import PythonOperator

class StarlakeAirflowBashJob(StarlakeAirflowJob):
    """Airflow Starlake Bash Job."""
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs):
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)

    @classmethod
    def sl_execution_environment(cls) -> Union[StarlakeExecutionEnvironment, str]:
        """Returns the execution environment to use.

        Returns:
            StarlakeExecutionEnvironment: The execution environment to use.
        """
        return StarlakeExecutionEnvironment.SHELL

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

        env = self.sl_env_vars.copy() # Copy the current sl env variables

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
                    options = ",".join([f"{key}={value}" for i, (key, value) in enumerate(temp.items())])
                    for opt in opts.split(","):
                        if "=" not in opt:
                            options += f",{opt}"
                else:
                    options = ",".join([f"{key}={value}" for i, (key, value) in enumerate(self.sl_env_vars.items())]) # Add/overwrite with sl env variables
                arguments[index+1] = options
                found = True
                break

        if not found:
            arguments.append("--options")
            arguments.append(",".join([f"{key}={value}" for key, value in self.sl_env_vars.items()])) # Add/overwrite with sl env variables

        command = __class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options) + f" {' '.join(arguments)}"
        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if kwargs.get('do_xcom_push', False):
            return PythonOperator(
                task_id=task_id,
                python_callable=self.execute_command,
                op_args=[command],
                op_kwargs=kwargs,
                provide_context=True,
                **kwargs
            )
        else:
            return BashOperator(
                task_id=task_id,
                bash_command=command,
                cwd=self.sl_root,
                env=env,
                **kwargs
            )
