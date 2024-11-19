from datetime import timedelta, datetime

from typing import Union, List

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig

from ai.starlake.airflow.starlake_airflow_options import StarlakeAirflowOptions

from ai.starlake.common import MissingEnvironmentVariable, sanitize_id
from airflow import DAG

from airflow.datasets import Dataset

from airflow.models.baseoperator import BaseOperator

from airflow.operators.dummy import DummyOperator

from airflow.operators.python import ShortCircuitOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from airflow.utils.task_group import TaskGroup

DEFAULT_POOL:str ="default_pool"

DEFAULT_DAG_ARGS = {
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
}

class StarlakeAirflowJob(IStarlakeJob[BaseOperator], StarlakeAirflowOptions):
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], options: dict=None, **kwargs) -> None:
        """Overrides IStarlakeJob.__init__()
        Args:
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The pre-load strategy to use.
            options (dict): The options to use.
        """
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.pool = str(__class__.get_context_var(var_name='default_pool', default_value=DEFAULT_POOL, options=self.options))
        self.outlets: List[Dataset] = kwargs.get('outlets', [])
        sd = __class__.get_context_var(var_name='start_date', default_value="2024-11-1", options=self.options)
        import re
        pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        if pattern.fullmatch(sd):
            from airflow.utils import timezone
            self.start_date = timezone.make_aware(datetime.strptime(sd, "%Y-%m-%d"))
        else:
            from airflow.utils.dates import days_ago
            self.start_date = days_ago(1)
        try:
            ed = __class__.get_context_var(var_name='end_date', options=self.options)
        except MissingEnvironmentVariable:
            ed = ""
        if pattern.fullmatch(ed):
            from airflow.utils import timezone
            self.end_date = timezone.make_aware(datetime.strptime(ed, "%Y-%m-%d"))
        else:
            self.end_date = None

    def sl_outlets(self, uri: str, **kwargs) -> List[Dataset]:
        """Returns a list of Airflow datasets from the specified uri.

        Args:
            uri (str): The uri of the dataset.

        Returns:
            List[Dataset]: The list of datasets.
        """
        dag: Union[DAG,None] = kwargs.get('dag', None)
        extra: dict = dict()
        if dag is not None:
            extra['source']= dag.dag_id

        dataset = Dataset(self.sl_dataset(uri, **kwargs), extra=extra)

        return kwargs.get('outlets', []) + [dataset]

    def sl_import(self, task_id: str, domain: str, tables: set=set(), **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_import()
        Generate the Airflow task that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id ({domain}_import by default).
            domain (str): The required domain to import.
            tables (set): The optional tables to import.

        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'doc': kwargs.get('doc', f'Import tables {",".join(list(tables or []))} within {domain}.')})
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        outlets = self.sl_outlets(domain, **kwargs)
        self.outlets += outlets
        kwargs.update({'outlets': outlets})
        return super().sl_import(task_id=task_id, domain=domain, tables=tables, **kwargs)

    def sl_pre_load(self, domain: str, tables: set=set(), pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, **kwargs) -> Union[BaseOperator, None]:
        """Overrides IStarlakeJob.sl_pre_load()
        Generate the Airflow group of tasks that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to pre-load.
            tables (set): The optional tables to pre-load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Union[BaseOperator, None]: The Airflow group of tasks or None.
        """
        if isinstance(pre_load_strategy, str):
            pre_load_strategy = \
                StarlakePreLoadStrategy(pre_load_strategy) if StarlakePreLoadStrategy.is_valid(pre_load_strategy) \
                    else self.pre_load_strategy

        pre_load_strategy = self.pre_load_strategy if not pre_load_strategy else pre_load_strategy

        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if pre_load_strategy == StarlakePreLoadStrategy.NONE:
            return self.pre_tasks(**kwargs)
        else:
            if pre_load_strategy == StarlakePreLoadStrategy.ACK:
                ack_wait_timeout = int(__class__.get_context_var(
                    var_name='ack_wait_timeout',
                    default_value=60*60, # 1 hour
                    options=self.options
                ))

                kwargs.update({'retry_delay': timedelta(seconds=ack_wait_timeout)})

            with TaskGroup(group_id=sanitize_id(f'{domain}_pre_load_tasks')) as pre_load_tasks:

                pre_tasks = self.pre_tasks(**kwargs)

                pre_load = super().sl_pre_load(
                    domain=domain, 
                    tables=tables, 
                    pre_load_strategy=pre_load_strategy, 
                    do_xcom_push=True, 
                    doc = f'Pre-load for tables {",".join(list(tables or []))} within {domain} using {pre_load_strategy.value} strategy.',
                    **kwargs
                )

                skip_or_start = ShortCircuitOperator(
                    doc = f"Skip or start loading tables {','.join(list(tables or []))} within {domain} domain.",
                    task_id = sanitize_id(f'{domain}_skip_or_start'),
                    python_callable = self.skip_or_start,
                    op_args=[pre_load],
                    op_kwargs=kwargs,
                    provide_context = True,
                    trigger_rule = 'all_done',
                    **kwargs
                )

                if pre_tasks:
                    pre_tasks >> pre_load

                pre_load >> skip_or_start

                if pre_load_strategy == StarlakePreLoadStrategy.IMPORTED:

                    import_task = self.sl_import(
                        task_id=sanitize_id(f'{domain}_import'),
                        domain=domain,
                        tables=tables,
                        **kwargs
                    )

                    skip_or_start >> import_task

            return pre_load_tasks

    def execute_command(self, command: str, **kwargs) -> int:
        """
        Execute the command and capture the return code.

        Args:
            command (str): The command to run.
            **kwargs: The optional keyword arguments.

        Returns:
            int: The return code.
        """
        env = self.sl_env(command)

        import subprocess
        try:
            # Run the command and capture the return code
            result = subprocess.run(
                args=command.split(' '), 
                env=env,
                check=True, 
                stderr=subprocess.STDOUT, 
                stdout=subprocess.PIPE,
            )
            return_code = result.returncode
            print(str(result.stdout, 'utf-8'))
            return return_code
        except subprocess.CalledProcessError as e:
            # Capture the return code in case of failure
            return_code = e.returncode
            # Push the return code to XCom
            kwargs['ti'].xcom_push(key='return_value', value=return_code)
            print(str(e.stdout, 'utf-8'))
            raise # Re-raise the exception to mark the task as failed

    def skip_or_start(self, upstream_task: BaseOperator, **kwargs) -> bool:
        """
        Args:
            upstream_task (BaseOperator): The upstream task.
            **kwargs: The optional keyword arguments.

        Returns:
            bool: True if the task should be started, False otherwise.
        """
        if isinstance(upstream_task, DataprocSubmitJobOperator):
            job = upstream_task.hook.get_job(
                job_id=upstream_task.job_id, 
                project_id=upstream_task.project_id, 
                region=upstream_task.region
            )
            state = job.status.state
            from google.cloud.dataproc_v1 import JobStatus
            if state == JobStatus.State.DONE:
                failed = False
            elif state == JobStatus.State.ERROR:
                failed = True
                print(f"Job failed:\n{job}")
            elif state == JobStatus.State.CANCELLED:
                failed = True
                print(f"Job was cancelled:\n{job}")
            else:
                from airflow.exceptions import AirflowException
                raise AirflowException(f"Job is still running:\n{job}")

        else:
            return_value = kwargs['ti'].xcom_pull(task_ids=upstream_task.task_id, key='return_value')

            print(f"Upstream task {upstream_task.task_id} return value: {return_value}[{type(return_value)}]")

            if return_value is None:
                failed = True
                print("No return value found in XCom.")
            elif isinstance(return_value, int):
                failed = return_value
                print(f"Return value: {failed}")
            elif isinstance(return_value, str):
                try:
                    import ast
                    parsed_return_value = ast.literal_eval(return_value)
                    if isinstance(parsed_return_value, int):
                        failed = parsed_return_value
                        print(f"Parsed return value: {failed}")
                    elif isinstance(parsed_return_value, str) and parsed_return_value:
                        failed = int(parsed_return_value.strip())
                        print(f"Parsed return value: {failed}")
                    else:
                        failed = True
                        print(f"Parsed return value {parsed_return_value}[{type(parsed_return_value)}] is not a valid integer or is empty.")
                except (ValueError, SyntaxError) as e:
                    failed = True
                    print(f"Error parsing return value: {e}")
            else:
                failed = True
                print("Return value is not a valid integer or string.")

        return not failed

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_load()
        Generate the Airflow task that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id ({domain}_{table}_load by default).
            domain (str): The required domain of the table to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
        
        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'doc': kwargs.get('doc', f'Load table {table} within {domain} domain.')})
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        outlets = self.sl_outlets(f'{domain}.{table}', **kwargs)
        self.outlets += outlets
        kwargs.update({'outlets': outlets})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Airflow task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The transform to run.
            transform_options (str): The optional transform options to use.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
        
        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'doc': kwargs.get('doc', f'Run {transform_name} transform.')})
        outlets = self.sl_outlets(transform_name, **kwargs)
        self.outlets += outlets
        kwargs.update({'outlets': outlets})
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, **kwargs)

    def dummy_op(self, task_id, **kwargs):
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return DummyOperator(task_id=task_id, **kwargs)

    def default_dag_args(self) -> dict:
        import json
        from json.decoder import JSONDecodeError
        dag_args = DEFAULT_DAG_ARGS
        try:
            dag_args.update(json.loads(__class__.get_context_var(var_name="default_dag_args", options=self.options)))
        except (MissingEnvironmentVariable, JSONDecodeError):
            pass
        dag_args.update({'start_date': self.start_date, 'retry_delay': timedelta(seconds=self.retry_delay), 'retries': self.retries})
        return dag_args