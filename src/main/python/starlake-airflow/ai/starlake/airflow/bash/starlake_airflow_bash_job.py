from typing import Optional, Union

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.job import StarlakePreLoadStrategy, StarlakeSparkConfig, StarlakeExecutionEnvironment, TaskType

from ai.starlake.airflow import StarlakeAirflowJob, StarlakeDatasetMixin

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

    def sl_job(self, task_id: str, arguments: list, spark_config: Optional[StarlakeSparkConfig] = None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType] = None, **kwargs) -> BaseOperator:
        """Overrides StarlakeAirflowJob.sl_job()
        Generate the Airflow task that will run the starlake command.

        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (Optional[StarlakeSparkConfig], optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType], optional): The optional task type. Defaults to None.

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

        preload = False
        if task_type and task_type==TaskType.PRELOAD:
            preload = True

        command = __class__.get_context_var("SL_STARLAKE_PATH", "starlake", self.options) + f" {' '.join(arguments)}"
        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if kwargs.get('do_xcom_push', False):
            if preload:
                command=f"""
                set -e
                bash -c '
                {command}
                return_code=$?

                # Push the return code to XCom
                echo $return_code

                '
                """
            else:
                command=f"""
                set -e
                bash -c '
                {command}
                return_code=$?

                # Push the return code to XCom
                echo $return_code

                # Exit with the captured return code if non-zero
                if [ $return_code -ne 0 ]; then
                    exit $return_code
                fi
                '
                """
        return StarlakeBashOperator(
            task_id=task_id,
            dataset=dataset,
            source=self.source,
            bash_command=command,
            cwd=self.sl_root,
            env=env,
            **kwargs
        )

class StarlakePythonOperator(StarlakeDatasetMixin, PythonOperator):
    """Starlake Python Operator."""
    def __init__(
            self, 
            task_id: str, 
            dataset: Optional[Union[StarlakeDataset, str]],
            source: Optional[str],
            python_callable, 
            **kwargs
        ) -> None:
        super().__init__(
            task_id=task_id, 
            dataset=dataset, 
            source=source, 
            python_callable=python_callable, 
            **kwargs
        )

class StarlakeBashOperator(StarlakeDatasetMixin, BashOperator):
    """Starlake Bash Operator."""
    def __init__(
            self, 
            task_id: str, 
            dataset: Optional[Union[StarlakeDataset, str]],
            source: Optional[str],
            bash_command: str, 
            **kwargs
        ):
        super().__init__(
            task_id=task_id, 
            dataset=dataset, 
            source=source, 
            bash_command=bash_command, 
            **kwargs
        )

