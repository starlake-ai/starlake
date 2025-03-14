from __future__ import annotations

from datetime import timedelta, datetime

from typing import Any, Optional, List, Union

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOrchestrator

from ai.starlake.airflow.starlake_airflow_options import StarlakeAirflowOptions

from ai.starlake.common import MissingEnvironmentVariable, sanitize_id

from ai.starlake.job.starlake_job import StarlakeOrchestrator

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from airflow.datasets import Dataset, DatasetAlias

from airflow.models.baseoperator import BaseOperator

from airflow.operators.empty import EmptyOperator

from airflow.operators.python import ShortCircuitOperator

from airflow.utils.task_group import TaskGroup

import logging

DEFAULT_POOL:str ="default_pool"

DEFAULT_DAG_ARGS = {
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5)
}

class AirflowDataset(AbstractEvent[Dataset]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> Dataset:
        extra = {}
        if source:
            extra["source"] = source
        return Dataset(dataset.refresh().url, extra)

class StarlakeAirflowJob(IStarlakeJob[BaseOperator, Dataset], StarlakeAirflowOptions, AirflowDataset):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], options: dict=None, **kwargs) -> None:
        """Overrides IStarlakeJob.__init__()
        Args:
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The pre-load strategy to use.
            options (dict): The options to use.
        """
        super().__init__(filename, module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
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

    @classmethod
    def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str]:
        return StarlakeOrchestrator.AIRFLOW

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
        return super().sl_import(task_id=task_id, domain=domain, tables=tables, **kwargs)

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
            if result.stdout is not None:
                print(result.stdout.decode('utf-8'))
            # if result.stderr is not None:
            #     stderr = result.stderr.decode('utf-8')
            #     print(stderr)
            return return_code
        except subprocess.CalledProcessError as e:
            # Capture the return code in case of failure
            return_code = e.returncode
            # Push the return code to XCom
            kwargs['ti'].xcom_push(key='return_value', value=return_code)
            output = e.output.decode('utf-8')
            print(output)
            # if e.stderr is not None:
            #     stderr = e.stderr.decode('utf-8')
            #     print(stderr)
            # Re-raise the exception to mark the task as failed
            raise 

    def start_op(self, task_id, scheduled: bool, not_scheduled_datasets: Optional[List[StarlakeDataset]], least_frequent_datasets: Optional[List[StarlakeDataset]], most_frequent_datasets: Optional[List[StarlakeDataset]], **kwargs) -> Optional[BaseOperator]:
        """Overrides IStarlakeJob.start_op()
        It represents the first task of a pipeline, it will define the optional condition that may trigger the DAG.
        Args:
            task_id (str): The required task id.
            scheduled (bool): whether the dag is scheduled or not.
            not_scheduled_datasets (Optional[List[StarlakeDataset]]): The optional not scheduled datasets.
            least_frequent_datasets (Optional[List[StarlakeDataset]]): The optional least frequent datasets.
            most_frequent_datasets (Optional[List[StarlakeDataset]]): The optional most frequent datasets.
        Returns:
            Optional[BaseOperator]: The optional Airflow task.
        """
        if not scheduled and least_frequent_datasets:
            with TaskGroup(group_id=f'{task_id}') as start:
                with TaskGroup(group_id=f'trigger_least_frequent_datasets') as trigger_least_frequent_datasets:
                    for dataset in least_frequent_datasets:
                        StarlakeEmptyOperator(
                            task_id=f"trigger_{dataset.uri}",
                            dataset=dataset,
                            previous=True,
                            source=self.source,
                            **kwargs.copy())
            return start 
        else:
            return super().start_op(task_id, scheduled, not_scheduled_datasets, least_frequent_datasets, most_frequent_datasets, **kwargs)

    def sl_pre_load(self, domain: str, tables: set=set(), pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, **kwargs) -> Optional[BaseOperator]:
        """Overrides IStarlakeJob.sl_pre_load()
        Generate the Airflow group of tasks that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to pre-load.
            tables (set): The optional tables to pre-load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Optional[BaseOperator]: The Airflow task or None.
        """
        pre_load_strategy = self.pre_load_strategy if not pre_load_strategy else pre_load_strategy
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        kwargs.update({'do_xcom_push': True})
        kwargs.update({'doc': kwargs.get('doc', f'Pre-load for tables {",".join(list(tables or []))} within {domain} using {pre_load_strategy.value} strategy.')})
        return super().sl_pre_load(domain=domain, tables=tables, pre_load_strategy=pre_load_strategy, **kwargs)

    def skip_or_start_op(self, task_id: str, upstream_task: BaseOperator, **kwargs) -> Optional[BaseOperator]:
        """
        Args:
            task_id (str): The required task id.
            upstream_task (BaseOperator): The upstream task.
            **kwargs: The optional keyword arguments.

        Returns:
            Optional[BaseOperator]: The Airflow task or None.
        """
        def f_skip_or_start(upstream_task_id: str, **kwargs) -> bool:
            logger = logging.getLogger(__name__)

            return_value = kwargs['ti'].xcom_pull(task_ids=upstream_task_id, key='return_value')

            logger.warning(f"Upstream task {upstream_task_id} return value: {return_value}[{type(return_value)}]")

            if return_value is None:
                failed = True
                logger.error("No return value found in XCom.")
            elif isinstance(return_value, bool):
                failed = not return_value
            elif isinstance(return_value, int):
                failed = return_value
            elif isinstance(return_value, str):
                try:
                    import ast
                    parsed_return_value = ast.literal_eval(return_value)
                    if isinstance(parsed_return_value, bool):
                        failed = not parsed_return_value
                    elif isinstance(parsed_return_value, int):
                        failed = parsed_return_value
                    elif isinstance(parsed_return_value, str) and parsed_return_value:
                        failed = int(parsed_return_value.strip())
                    else:
                        failed = True
                        logger.error(f"Parsed return value {parsed_return_value}[{type(parsed_return_value)}] is not a valid bool, integer or is empty.")
                except (ValueError, SyntaxError) as e:
                    failed = True
                    logger.error(f"Error parsing return value: {e}")
            else:
                failed = True
                logger.error("Return value is not a valid bool, integer or string.")

            logger.warning(f"Failed: {failed}")

            return not failed

        kwargs.update({'pool': kwargs.get('pool', self.pool)})

        if not isinstance(upstream_task, BaseOperator):
            raise ValueError("The upstream task must be an instance of BaseOperator.")
        upstream_task_id = upstream_task.task_id
        task_id = task_id or f"validating_{upstream_task_id.split('.')[-1]}"
        kwargs.pop("task_id", None)

        return ShortCircuitOperator(
            task_id = task_id,
            python_callable = f_skip_or_start,
            op_args=[upstream_task_id],
            op_kwargs=kwargs,
            trigger_rule = 'all_done',
            **kwargs
        )

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: Optional[StarlakeSparkConfig] = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_load()
        Generate the Airflow task that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id ({domain}_{table}_load by default).
            domain (str): The required domain of the table to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Optional[Union[StarlakeDataset, str]]): The optional dataset to materialize.
        
        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'doc': kwargs.get('doc', f'Load table {table} within {domain} domain.')})
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, spark_config: Optional[StarlakeSparkConfig] = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> BaseOperator:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Airflow task that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The transform to run.
            transform_options (str): The optional transform options to use.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Optional[Union[StarlakeDataset, str]]): The optional dataset to materialize.
        
        Returns:
            BaseOperator: The Airflow task.
        """
        kwargs.update({'doc': kwargs.get('doc', f'Run {transform_name} transform.')})
        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, dataset=dataset,  **kwargs)

    def dummy_op(self, task_id, events: Optional[List[Dataset]] = None, **kwargs) -> BaseOperator :
        """Dummy op.
        Generate a Airflow dummy op.

        Args:
            task_id (str): The required task id.
            events (Optional[List[Dataset]]): The optional events to materialize.

        Returns:
            BaseOperator: The Airflow task.
        """

        kwargs.update({'pool': kwargs.get('pool', self.pool)})
        outlets: List[Dataset] = kwargs.get("outlets", [])
        if events:
            outlets += events
        kwargs.update({'outlets': outlets})
        return EmptyOperator(task_id=task_id, **kwargs)

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

from airflow.lineage import prepare_lineage, apply_lineage
from airflow.utils.log.logging_mixin import LoggingMixin

class StarlakeDatasetMixin(LoggingMixin):
    """Mixin to update Airflow outlets with Starlake datasets."""
    def __init__(self, 
                 task_id: str, 
                 dataset: Optional[Union[str, StarlakeDataset]] = None, 
                 previous:bool= False, 
                 source: Optional[str] = None, 
                 **kwargs
                 ) -> None:
        self.task_id = task_id
        params: dict = kwargs.get("params", dict())
        outlets = kwargs.get("outlets", [])
        extra = params
        extra.update({"source": source})
        self.extra = extra
        self.alias = sanitize_id(params.get("uri", task_id)).lower()
        if dataset:
            if isinstance(dataset, StarlakeDataset):
                params.update({
                    'uri': dataset.uri,
                    'sl_schedule': dataset.cron,
                    'sl_schedule_parameter_name': dataset.sl_schedule_parameter_name, 
                    'sl_schedule_format': dataset.sl_schedule_format,
                    'previous': previous
                })
                kwargs['params'] = params
                self.dataset = "{{sl_scheduled_dataset(params.uri, params.sl_schedule, data_interval_end | ts, params.sl_schedule_parameter_name, params.sl_schedule_format, params.previous)}}"
            else:
                self.dataset = dataset
            outlets.append(DatasetAlias(self.alias))
            kwargs["outlets"] = outlets
            self.outlets = outlets
            self.template_fields = getattr(self, "template_fields", tuple()) + ("dataset",)
        else:
            self.dataset = None
        super().__init__(task_id=task_id, **kwargs)  # Appelle l'init de l'opérateur principal

    @prepare_lineage
    def pre_execute(self, context):
        if self.dataset:
            self.log.info(f"Pre execute {self.task_id} with dataset={self.dataset}, alias={self.alias}, extra={self.extra}, outlets={self.outlets}")
            from urllib.parse import parse_qs
            uri: str = self.render_template(self.dataset, context)
            query = uri.split("?")
            if query.__len__() > 1:
                self.extra.update(parse_qs(query[-1]))
            context["outlet_events"][self.alias].add(Dataset(uri, self.extra))
        return super().pre_execute(context)

    @apply_lineage
    def post_execute(self, context: Any, result: Any = None):
        """
        Execute right after self.execute() is called.

        It is passed the execution context and any results returned by the operator.
        """
        if self.dataset:
            context["outlet_events"][self.alias].extra = self.extra
        return super().post_execute(context, result)

class StarlakeEmptyOperator(StarlakeDatasetMixin, EmptyOperator):
    """StarlakeEmptyOperator."""
    def __init__(self, 
                 task_id: str, 
                 dataset: Optional[Union[str, StarlakeDataset]] = None, 
                 source: Optional[str] = None, 
                 **kwargs
        ) -> None:
        super().__init__(
            task_id=task_id, 
            dataset=dataset,
            source=source,
            **kwargs
        )