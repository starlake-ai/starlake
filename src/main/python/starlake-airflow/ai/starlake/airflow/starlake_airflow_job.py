from __future__ import annotations

from datetime import timedelta, datetime

from typing import Optional, List, Union

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOrchestrator, TaskType

from ai.starlake.airflow.starlake_airflow_options import StarlakeAirflowOptions

from ai.starlake.common import MissingEnvironmentVariable

from ai.starlake.job.starlake_job import StarlakeOrchestrator

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from airflow.datasets import Dataset

from airflow.models.baseoperator import BaseOperator

from airflow.models.dataset import DatasetEvent

from airflow.operators.empty import EmptyOperator

from airflow.operators.python import ShortCircuitOperator

from airflow.utils.context import Context

from airflow.utils.task_group import TaskGroup

import logging

DEFAULT_POOL:str ="default_pool"

DEFAULT_DAG_ARGS = {
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

def __check_version__(version: str) -> bool:
    """
    Check if the current version is compatible with the given version.
    """
    from packaging.version import parse
    import airflow

    current_version = parse(airflow.__version__)

    return current_version >= parse(version)

def supports_inlet_events():
    """
    Check if the current environment supports inlet events.
    """
    return __check_version__("2.10.0")

def supports_assets():
    """
    Check if the current environment supports assets.
    """
    return __check_version__("3.0.0")

class AirflowDataset(AbstractEvent[Dataset]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> Dataset:
        extra = {
            "uri": dataset.uri,
            "cron": dataset.cron,
            "freshness": dataset.freshness,
        }
        if source:
            extra["source"] = source
        if not supports_inlet_events():
            return Dataset(dataset.refresh().url, extra)
        return Dataset(dataset.uri, extra)

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
        if not supports_inlet_events() and not scheduled and least_frequent_datasets:
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
        elif supports_inlet_events() and not scheduled:
            datasets: List[Dataset] = []
            datasets += list(map(lambda dataset: self.to_event(dataset=dataset), not_scheduled_datasets or []))
            datasets += list(map(lambda dataset: self.to_event(dataset=dataset), least_frequent_datasets or []))
            datasets += list(map(lambda dataset: self.to_event(dataset=dataset), most_frequent_datasets or []))

            def get_scheduled_datetime(dataset: Dataset) -> Optional[datetime]:
                extra = dataset.extra or {}
                scheduled_date = extra.get("scheduled_date", None)
                if scheduled_date:
                    from dateutil import parser
                    import pytz
                    dt = parser.isoparse(scheduled_date).astimezone(pytz.timezone('UTC'))
                    extra.update({"scheduled_datetime": dt})
                    dataset.extra = extra
                    return dt
                return None

            def get_triggering_datasets(context: Context = None) -> List[Dataset]:

                if not context:
                    from airflow.operators.python import get_current_context
                    context = get_current_context()

                uri: str
                event: DatasetEvent
                datasets: List[Dataset] = []

                triggering_dataset_events = context['task_instance'].get_template_context()["triggering_dataset_events"]
                triggering_uris = {}
                for uri, events in triggering_dataset_events.items():
                    if not isinstance(events, list):
                        continue

                    for event in events:
                        if not isinstance(event, DatasetEvent):
                            continue

                        extra = event.extra or {}
                        if not extra.get("ts", None):
                            extra.update({"ts": event.timestamp})
                        ds = Dataset(uri=uri, extra=extra)
                        if uri not in triggering_uris:
                            triggering_uris[uri] = event
                            datasets.append(ds)
                        else:
                            previous_event: DatasetEvent = triggering_uris[uri]
                            if event.timestamp > previous_event.timestamp:
                                triggering_uris[uri] = event
                                datasets.append(ds)

                return datasets

            def check_datasets(scheduled_date: datetime, datasets: List[Dataset], context: Context) -> bool:
                from croniter import croniter
                missing_datasets = []
                inlet_events = context['task_instance'].get_template_context()['inlet_events']
                for dataset in datasets:
                    dataset_events = inlet_events.get(dataset, None)
                    extra = dataset.extra or {}
                    cron = extra.get("cron", None)
                    freshness = extra.get("freshness", 0)
                    if cron:
                        iter = croniter(cron, scheduled_date)
                        curr = iter.get_current(datetime)
                        previous = iter.get_prev(datetime)
                        next = croniter(cron, previous).get_next(datetime)
                        if curr == next :
                            scheduled_date_to_check = curr
                        else:
                            scheduled_date_to_check = previous
                        scheduled_date_to_check_min = scheduled_date_to_check - timedelta(seconds=freshness)
                        scheduled_date_to_check_max = scheduled_date_to_check + timedelta(seconds=freshness)
                        scheduled_datetime = extra.get("scheduled_datetime", None)
                        if scheduled_datetime:
                            if freshness > 0:
                                if scheduled_date_to_check_min > scheduled_datetime or scheduled_datetime > scheduled_date_to_check_max:
                                    missing_datasets.append(dataset)
                                    print(f"Triggering dataset {dataset.uri} with scheduled datetime {scheduled_datetime} not between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                else:
                                    print(f"Found trigerring dataset {dataset.uri} with scheduled datetime {scheduled_datetime} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                            elif scheduled_datetime != scheduled_date_to_check:
                                missing_datasets.append(dataset)
                                print(f"Triggering dataset {dataset.uri} with scheduled datetime {scheduled_datetime} != {scheduled_date_to_check}")
                            else:
                                print(f"Triggering dataset {dataset.uri} with scheduled datetime {scheduled_datetime} == {scheduled_date_to_check} found")
                        else:
                            dataset_events = inlet_events.get(dataset, [])
                            dataset_event: Optional[DatasetEvent] = None
                            nb_events = len(dataset_events)
                            i = 1
                            # we check the dataset events in reverse order
                            while i < nb_events and not dataset_event:
                                event: DatasetEvent = dataset_events[-i]
                                extra = event.extra or {}
                                scheduled_datetime = get_scheduled_datetime(Dataset(uri=dataset.uri, extra=extra))
                                if scheduled_datetime and freshness > 0:
                                    if scheduled_date_to_check_min > scheduled_datetime or scheduled_datetime > scheduled_date_to_check_max:
                                        print(f"Dataset event for {dataset.uri} with scheduled datetime {scheduled_datetime} not between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                        i += 1
                                    else:
                                        print(f"Dataset event for {dataset.uri} with scheduled datetime {scheduled_datetime} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max} found")
                                        dataset_event = event
                                        break;
                                elif scheduled_datetime and scheduled_datetime == scheduled_date_to_check:
                                    print(f"Dataset event for {dataset.uri} with scheduled datetime {scheduled_datetime} found")
                                    dataset_event = event
                                    break;
                                else:
                                    print(f"Dataset event for {dataset.uri} with scheduled datetime {scheduled_datetime} != {scheduled_date_to_check}")
                                    i += 1
                            if not dataset_event:
                                missing_datasets.append(dataset)
                    elif not dataset_events:
                        print(f"No dataset events for {dataset.uri} found")
                        missing_datasets.append(dataset)
                    else:
                        print(f"Found dataset event for {dataset.uri} not scheduled")
                checked = not missing_datasets
                if checked:
                    print(f"All datasets checked: {', '.join([dataset.uri for dataset in datasets])}")
                    context['task_instance'].xcom_push(key='sl_logical_date', value=scheduled_date)
                return checked

            def should_continue(**context) -> bool:
                triggering_datasets = get_triggering_datasets(context)
                if not triggering_datasets:
                    print("No triggering datasets found. Manually triggered.")
                    return True
                # if triggering_datasets.__len__() == datasets.__len__():
                #     return True
                else:
                    triggering_uris = {dataset.uri: dataset for dataset in triggering_datasets}
                    datasets_uris = {dataset.uri: dataset for dataset in datasets}
                    # we first retrieve the scheduled datetime of all the triggering datasets
                    triggering_scheduled = {dataset.uri: get_scheduled_datetime(dataset) for dataset in triggering_datasets}
                    # then we retrieve the triggering dataset with the greatest scheduled datetime
                    greatest_triggering_dataset: tuple = max(triggering_scheduled.items(), key=lambda x: x[1], default=(None, None))
                    greatest_triggering_dataset_uri = greatest_triggering_dataset[0]
                    greatest_triggering_dataset_datetime = greatest_triggering_dataset[1]
                    # we then check the other datasets
                    checking_uris = list(set(datasets_uris.keys()) - set(greatest_triggering_dataset_uri))
                    checking_triggering_datasets = [dataset for dataset in triggering_datasets if dataset.uri in checking_uris]
                    checking_missing_datasets = [dataset for dataset in datasets if dataset.uri in list(set(checking_uris) - set(triggering_uris.keys()))]
                    checking_datasets = checking_triggering_datasets + checking_missing_datasets
                    return check_datasets(greatest_triggering_dataset_datetime, checking_datasets, context)

            inlets = kwargs.get("inlets", [])
            inlets += datasets
            kwargs.update({'inlets': inlets})
            kwargs.update({'doc': kwargs.get('doc', f'Check if the DAG should be triggered.')})
            kwargs.update({'pool': kwargs.get('pool', self.pool)})
            kwargs.update({'do_xcom_push': True})
            return ShortCircuitOperator(
                task_id = task_id,
                python_callable = should_continue,
                op_args=[],
                op_kwargs=kwargs,
                trigger_rule = 'all_done',
                **kwargs
            )
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

    def dummy_op(self, task_id, events: Optional[List[Dataset]] = None, task_type: Optional[TaskType] = TaskType.EMPTY, **kwargs) -> BaseOperator :
        """Dummy op.
        Generate a Airflow dummy op.

        Args:
            task_id (str): The required task id.
            events (Optional[List[Dataset]]): The optional events to materialize.
            task_type (Optional[TaskType]): The optional task type.

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

from airflow.lineage import prepare_lineage

class StarlakeDatasetMixin:
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
        extra = dict()
        extra.update({"source": source})
        self.ts = "{{ data_interval_end | ts }}"
        if dataset:
            if isinstance(dataset, StarlakeDataset):
                params.update({
                    'uri': dataset.uri,
                    'cron': dataset.cron,
                    'sl_schedule_parameter_name': dataset.sl_schedule_parameter_name, 
                    'sl_schedule_format': dataset.sl_schedule_format,
                    'previous': previous
                })
                kwargs['params'] = params
                extra.update({
                    'uri': dataset.uri,
                    'cron': dataset.cron,
                    'sink': dataset.sink,
                    'freshness': dataset.freshness,
                })
                if dataset.cron: # if the dataset is scheduled
                    self.scheduled_dataset = "{{sl_scheduled_dataset(params.uri, params.cron, ts_as_datetime(data_interval_end | ts), params.sl_schedule_parameter_name, params.sl_schedule_format, params.previous)}}"
                else:
                    self.scheduled_dataset = None
                self.scheduled_date = "{{sl_scheduled_date(params.cron, ts_as_datetime(data_interval_end | ts), params.previous)}}"
                uri = dataset.uri
            else:
                self.scheduled_dataset = None
                uri = dataset
            outlets.append(Dataset(uri=uri, extra=extra))
            kwargs["outlets"] = outlets
            self.template_fields = getattr(self, "template_fields", tuple()) + ("scheduled_dataset", "scheduled_date", "ts",)
        else:
            self.scheduled_dataset = None
        self.extra = extra
        super().__init__(task_id=task_id, **kwargs)  # Appelle l'init de l'opérateur principal

    @prepare_lineage
    def pre_execute(self, context):
        self.extra.update({"ts": self.ts})
        if self.scheduled_date:
            self.extra.update({"scheduled_date": self.scheduled_date})
        if self.scheduled_dataset:
            dataset = Dataset(uri=self.scheduled_dataset, extra=self.extra)
            self.outlets.append(dataset)
        for outlet in self.outlets:
            outlet_event = context["outlet_events"][outlet]
            self.log.info(f"updating outlet event {outlet_event} with extra {self.extra}")
            outlet_event.extra = self.extra
        return super().pre_execute(context)

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