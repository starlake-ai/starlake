import re
from datetime import timedelta, datetime

from typing import Union, List

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig

from ai.starlake.airflow.starlake_airflow_options import StarlakeAirflowOptions

from ai.starlake.common import keep_ascii_only, sanitize_id

from airflow.datasets import Dataset

from airflow.models.baseoperator import BaseOperator

from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from airflow.operators.bash import BashOperator

from airflow.operators.dummy import DummyOperator

from airflow.operators.python import ShortCircuitOperator

from airflow.sensors.filesystem import FileSensor

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

    def sl_import(self, task_id: str, domain: str, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_import()
        Generate the Airflow task that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id ({domain}_import by default).
            domain (str): The required domain to import.

        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        dataset = Dataset(keep_ascii_only(domain).lower())
        self.outlets += kwargs.get('outlets', []) + [dataset]
        return super().sl_import(task_id=task_id, domain=domain, **kwargs)

    def sl_pre_load(self, domain: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, **kwargs) -> Union[BaseOperator, None]:
        """Overrides IStarlakeJob.sl_pre_load()
        Generate the Airflow group of tasks that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to load.
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

        if pre_load_strategy == StarlakePreLoadStrategy.IMPORTED:

            with TaskGroup(group_id=sanitize_id(f'{domain}_pre_load_tasks')) as pre_load_tasks:
                pre_tasks = self.pre_tasks(**kwargs)

                incoming_path = __class__.get_context_var(
                    var_name='incoming_path',
                    default_value=f'{self.sl_root}/incoming',
                    options=self.options
                )
                list_files_command = f'ls {incoming_path}/{domain}/* | wc -l'
                if incoming_path.startswith('gs://'):
                    list_files_command = "gsutil " + list_files_command
                list_files = BashOperator(
                    task_id=sanitize_id(f'{domain}_list_files'),
                    bash_command=list_files_command,
                    do_xcom_push=True,
                    **kwargs
                )

                def f_skip_or_start(**kwargs):
                    task_instance = kwargs['ti']
                    files_tuple = task_instance.xcom_pull(key=None, task_ids=[list_files.task_id])
                    print('Number of files found: {}'.format(files_tuple))
                    files_number = files_tuple[0]
                    return int(files_number) > 1

                skip_or_start = ShortCircuitOperator(
                    task_id = sanitize_id(f'{domain}_skip_or_start'),
                    python_callable = f_skip_or_start,
                    provide_context = True,
                    **kwargs
                )

                list_files >> skip_or_start

                import_task = self.sl_import(
                    task_id=sanitize_id(f'{domain}_import'),
                    domain=domain,
                    **kwargs
                )

                if pre_tasks:
                    skip_or_start >> pre_tasks >> import_task
                else:
                    skip_or_start >> import_task


            return pre_load_tasks

        elif pre_load_strategy == StarlakePreLoadStrategy.PENDING:
            with TaskGroup(group_id=sanitize_id(f'{domain}_pre_load_tasks')) as pre_load_tasks:
                pre_tasks = self.pre_tasks(**kwargs)

                pending_path = __class__.get_context_var(
                    var_name='pending_path',
                    default_value=f'{self.sl_datasets}/pending',
                    options=self.options
                )
                list_files_command = f'ls {pending_path}/{domain}/* | wc -l'
                if pending_path.startswith('gs://'):
                    list_files_command = "gsutil " + list_files_command
                list_files = BashOperator(
                    task_id=sanitize_id(f'{domain}_list_files'),
                    bash_command=list_files_command,
                    do_xcom_push=True,
                    **kwargs
                )

                def f_skip_or_start(**kwargs):
                    task_instance = kwargs['ti']
                    files_tuple = task_instance.xcom_pull(key=None, task_ids=[list_files.task_id])
                    files_number = int(str(files_tuple[0]).strip())
                    print('Number of files found: {}'.format(files_number))
                    return files_number > 1

                skip_or_start = ShortCircuitOperator(
                    task_id = sanitize_id(f'{domain}_skip_or_start'),
                    python_callable = f_skip_or_start,
                    provide_context = True,
                    **kwargs
                )

                if pre_tasks:
                    list_files >> skip_or_start >> pre_tasks
                else:
                    list_files >> skip_or_start

            return pre_load_tasks

        elif pre_load_strategy == StarlakePreLoadStrategy.ACK:
            with TaskGroup(group_id=sanitize_id(f'{domain}_pre_load_tasks')) as pre_load_tasks:
                pre_tasks = self.pre_tasks(**kwargs)

                ack_wait_timeout = int(__class__.get_context_var(
                    var_name='ack_wait_timeout',
                    default_value=60*60, # 1 hour
                    options=self.options
                ))

                ack_file = __class__.get_context_var(
                    var_name='global_ack_file_path',
                    default_value=f'{self.sl_datasets}/pending/{domain}/{{{{ds}}}}.ack',
                    options=self.options
                )

                gcs_result = re.search(r"gs://(.+?)/(.+)", ack_file)
                gcs_ack_bucket = gcs_result.group(1) if gcs_result else None
                gcs_ack_file = gcs_result.group(2) if gcs_result else None

                wait_for_ack = GCSObjectExistenceSensor(
                    task_id=sanitize_id(f'{domain}_wait_for_ack'),
                    bucket=gcs_ack_bucket,
                    object=gcs_ack_file,
                    timeout=ack_wait_timeout,
                    poke_interval=60,
                    mode='reschedule',
                    soft_fail=True,
                    **kwargs
                ) if gcs_result \
                    else FileSensor(
                            task_id=sanitize_id(f'{domain}_wait_for_ack'),
                            filepath=ack_file,
                            timeout=ack_wait_timeout,
                            poke_interval=60,
                            mode='reschedule',
                            soft_fail=True,
                            **kwargs
                        )

                delete_ack = GCSDeleteObjectsOperator(
                    task_id=sanitize_id(f'{domain}_delete_ack'),
                    bucket_name=gcs_ack_bucket,
                    objects=[gcs_ack_file],
                    **kwargs
                ) if gcs_result \
                    else BashOperator(
                        task_id=sanitize_id(f'{domain}_delete_ack'),
                        bash_command=f'rm {ack_file}',
                        **kwargs
                    )

                if pre_tasks:
                    wait_for_ack >> delete_ack >> pre_tasks
                else:
                    wait_for_ack >> delete_ack

            return pre_load_tasks

        else:
            return self.pre_tasks(**kwargs)

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None,**kwargs) -> BaseOperator:
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
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        dataset = Dataset(keep_ascii_only(f'{domain}.{table}').lower())
        self.outlets += kwargs.get('outlets', []) + [dataset]
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
        dataset = Dataset(keep_ascii_only(transform_name).lower())
        self.outlets += kwargs.get('outlets', []) + [dataset]
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, **kwargs)

    def dummy_op(self, task_id, **kwargs):
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return DummyOperator(task_id=task_id, **kwargs)
