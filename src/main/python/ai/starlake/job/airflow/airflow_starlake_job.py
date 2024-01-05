import os
import re
from datetime import timedelta, datetime

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob

from ai.starlake.common import keep_ascii_only, MissingEnvironmentVariable, sanitize_id, TODAY

from airflow.datasets import Dataset

from airflow.models import Variable

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

class AirflowStarlakeJob(IStarlakeJob[BaseOperator]):
    def __init__(self, pre_load_strategy: StarlakePreLoadStrategy|str|None, options: dict=None, **kwargs) -> None:
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        self.pool = str(__class__.get_context_var(var_name='default_pool', default_value=DEFAULT_POOL, options=self.options))
        self.outlets = kwargs.get('outlets', [])

    def sl_import(self, task_id: str, domain: str, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_import()"""
        task_id = f"{domain}_import" if not task_id else task_id
        arguments = ["import", "--includes", domain]
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        self.outlets += kwargs.get('outlets', []) + [Dataset(keep_ascii_only(domain))]
        return self.sl_job(task_id=task_id, arguments=arguments, **kwargs)

    def sl_pre_load(self, domain: str, pre_load_strategy: StarlakePreLoadStrategy|str|None=None, **kwargs) -> BaseOperator|None:
        """Overrides IStarlakeJob.sl_pre_load()"""
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
                    print('Number of files found: {}'.format(files_tuple))
                    files_number = files_tuple[0]
                    return int(files_number) > 1

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
                    default_value=f'{self.sl_datasets}/pending/{domain}/{TODAY}.ack',
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

    def sl_load(self, task_id: str, domain: str, table: str, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_load()"""
        task_id = f"{domain}_{table}_load" if not task_id else task_id
        arguments = ["load", "--domains", domain, "--tables", table]
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        self.outlets += kwargs.get('outlets', []) + [Dataset(keep_ascii_only(f'{domain}.{table}'))]
        return self.sl_job(task_id=task_id, arguments=arguments, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_transform()"""
        task_id = f"{transform_name}" if not task_id else task_id
        arguments = ["transform", "--name", transform_name]
        transform_options = transform_options if transform_options else __class__.get_context_var(transform_name, {}, self.options).get("options")
        if transform_options:
            arguments.extend(["--options", transform_options])
        self.outlets += kwargs.get('outlets', []) + [Dataset(keep_ascii_only(transform_name))]
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return self.sl_job(task_id=task_id, arguments=arguments, **kwargs)

    def dummy_op(self, task_id, **kwargs):
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return DummyOperator(task_id=task_id, **kwargs)

    @classmethod
    def get_context_var(cls, var_name: str, default_value: any=None, options: dict = None, **kwargs):
        """Overrides IStarlakeJob.get_context_var()"""
        if options and options.get(var_name):
            return options.get(var_name)
        elif default_value is not None:
            return default_value
        elif Variable.get(var_name, default_var=None, **kwargs) is not None:
            return Variable.get(var_name)
        elif os.getenv(var_name) is not None:
            return os.getenv(var_name)
        else:
            raise MissingEnvironmentVariable(f"{var_name} does not exist")
