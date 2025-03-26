from typing import List, Optional, Tuple, Union

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, TaskType

from ai.starlake.common import cron_start_time, is_valid_cron, sl_cron_start_end_dates, sl_schedule_format

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from dagster import AssetKey, Output, Out, op, AssetMaterialization

from dagster._core.definitions import NodeDefinition

from datetime import datetime

import pytz

class DagsterDataset(AbstractEvent[AssetKey]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> AssetKey:
        return AssetKey(dataset.refresh().url)

class StarlakeDagsterJob(IStarlakeJob[NodeDefinition, AssetKey], StarlakeOptions, DagsterDataset):
    def __init__(self, filename: str, module_name: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        now = datetime.now().strftime('%Y-%m-%d')
        sd = __class__.get_context_var(var_name='start_date', default_value=now, options=self.options)
        import re
        pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        if pattern.fullmatch(sd):
            start_date = datetime.strptime(sd, "%Y-%m-%d").astimezone(pytz.timezone('UTC'))
        else:
            raise ValueError(f"Invalid start date: {sd}")
        self.__start_date = start_date

    @property
    def start_date(self) -> datetime:
        return self.__start_date

    @classmethod
    def sl_orchestrator(cls) -> Union[StarlakeOrchestrator, str]:
         return StarlakeOrchestrator.DAGSTER

    def sl_pre_load(self, domain: str, tables: set=set(), pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, **kwargs) -> Optional[NodeDefinition]:
        """Overrides IStarlakeJob.sl_pre_load()
        Generate the Dagster node that will check if the conditions are met to load the specified domain according to the pre-load strategy choosen.

        Args:
            domain (str): The required domain to pre-load.
            tables (set): The optional tables to pre-load.
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The optional pre-load strategy to use.
        
        Returns:
            Optional[NodeDefinition]: The Dagster node or None.
        """

        if isinstance(pre_load_strategy, str):
            pre_load_strategy = \
                StarlakePreLoadStrategy(pre_load_strategy) if StarlakePreLoadStrategy.is_valid(pre_load_strategy) \
                    else self.pre_load_strategy

        pre_load_strategy = self.pre_load_strategy if not pre_load_strategy else pre_load_strategy

        if pre_load_strategy != StarlakePreLoadStrategy.NONE:
            kwargs.update({'out': 'succeeded', 'failure': 'failed',})

        return super().sl_pre_load(domain=domain, tables=tables, pre_load_strategy=pre_load_strategy, **kwargs)

    def sl_import(self, task_id: str, domain: str, tables: set=set(), **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_import()
        Generate the Dagster node that will run the starlake `import` command.

        Args:
            task_id (str): The optional task id ({domain}_import by default).
            domain (str): The required domain to import.
            tables (set): The optional tables to import.

        Returns:
            NodeDefinition: The Dagster node.
        """
        kwargs.update({'description': f"Starlake domain '{domain}' imported"})
        return super().sl_import(task_id=task_id, domain=domain, tables=tables, **kwargs)

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_load()
        Generate the Dagster node that will run the starlake `load` command.

        Args:
            task_id (str): The optional task id ({domain}_{table}_load by default).
            domain (str): The required domain to load.
            table (str): The required table to load.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.        
        """
        kwargs.update({'description': f"Starlake table '{domain}.{table}' loaded"})
        return super().sl_load(task_id=task_id, domain=domain, table=table, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, dataset: Optional[Union[StarlakeDataset, str]]= None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_transform()
        Generate the Dagster node that will run the starlake `transform` command.

        Args:
            task_id (str): The optional task id ({transform_name} by default).
            transform_name (str): The required transform name.
            transform_options (str, optional): The optional transform options. Defaults to None.
            spark_config (StarlakeSparkConfig, optional): The optional spark configuration. Defaults to None.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.

        Returns:
            NodeDefinition: The Dagster node.
        """
        kwargs.update({'description': f"Starlake transform '{transform_name}' executed"})
        return super().sl_transform(task_id=task_id, transform_name=transform_name, transform_options=transform_options, spark_config=spark_config, dataset=dataset, **kwargs)

    def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, dataset: Optional[Union[StarlakeDataset, str]]= None, task_type: Optional[TaskType]=None, **kwargs) -> NodeDefinition:
        """Overrides IStarlakeJob.sl_job()
        Generate the Dagster node that will run the starlake command.
        
        Args:
            task_id (str): The required task id.
            arguments (list): The required arguments of the starlake command to run.
            spark_config (StarlakeSparkConfig): The optional spark configuration to use.
            dataset (Optional[Union[StarlakeDataset, str]], optional): The optional dataset to materialize. Defaults to None.
            task_type (Optional[TaskType]): The optional task type to use.
        
        Returns:
            NodeDefinition: The Dagster node.
        """

    def dummy_op(self, task_id: str, events: Optional[List[AssetKey]] = None, task_type: Optional[TaskType] = TaskType.EMPTY, **kwargs) -> NodeDefinition:
        """Dummy op.
        Generate a Dagster dummy op.

        Args:
            task_id (str): The required task id.
            events (Optional[List[AssetKey]]): The optional events to materialize.
            task_type (Optional[TaskType]): The optional task type.

        Returns:
            NodeDefinition: The Dagster node.
        """

        out:str = kwargs.get("out", "result")

        assets: List[AssetKey] = kwargs.get("assets", [])
        if events:
            assets += events

        @op(
            name=task_id,
            required_resource_keys=set(),
            ins=kwargs.get("ins", {}),
            out={out: Out(dagster_type=str, is_required=True)}
        )
        def dummy(context, config: DagsterLogicalDatetimeConfig, **kwargs):
            yield Output(value=task_id, output_name=out)

            for asset in assets:
                yield AssetMaterialization(asset_key=asset.path, description=kwargs.get("description", f"Dummy op {task_id} execution succeeded"))

        return dummy

from dagster import Config

class DagsterLogicalDatetimeConfig(Config):
    logical_datetime: Optional[str]
    dry_run: bool = False

class StarlakeDagsterUtils:

    @classmethod
    def get_logical_datetime(cls, context, config: DagsterLogicalDatetimeConfig, **kwargs) -> datetime:
        """Get the logical datetime.

        Returns:
            datetime: The logical datetime.
        """
        context.log.info(f"config -> {config}")
        try:
            partition_key = context.partition_key
        except Exception as e:
            context.log.warning(e)
            partition_key = None
        if partition_key:
            context.log.info(f"Partition key: {partition_key}")
            from dateutil import parser
            logical_datetime = parser.isoparse(partition_key).astimezone(pytz.timezone('UTC'))
        elif config.logical_datetime:
            from dateutil import parser
            logical_datetime = parser.isoparse(config.logical_datetime).astimezone(pytz.timezone('UTC'))
        else:
            run_stats = context.instance.get_run_stats(context.dagster_run.run_id)._asdict()
            launch_time = run_stats.get('launch_time')
            if not launch_time:
                logical_datetime = datetime.now().astimezone(pytz.timezone('UTC'))
            else:
                logical_datetime = datetime.fromtimestamp(run_stats.get('launch_time')).astimezone(pytz.timezone('UTC'))
        context.log.info(f"logical datetime : {logical_datetime}")
        return logical_datetime

    @classmethod
    def get_asset(cls, context, config: DagsterLogicalDatetimeConfig, dataset: Union[StarlakeDataset, str], **kwargs) -> AssetKey:
        """Get the asset.

        Args:
            context (Context): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            dataset (Union[StarlakeDataset, str): The dataset.

        Returns:
            AssetKey: The asset.
        """
        if isinstance(dataset, str):
            return AssetKey(dataset)
        return AssetKey(dataset.refresh(cls.get_logical_datetime(context, config, **kwargs)).url)

    @classmethod
    def get_assets(cls, context, datasets: List[StarlakeDataset], config: DagsterLogicalDatetimeConfig, **kwargs) -> List[AssetKey]:
        """Get the assets.

        Args:
            context (Context): The context.
            datasets (List[StarlakeDataset]): The datasets.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.

        Returns:
            List[AssetKey]: The assets.
        """
        return [cls.get_asset(context, dataset, config, **kwargs) for dataset in datasets]

    @classmethod
    def get_transform_options(cls, context, config: DagsterLogicalDatetimeConfig, params: dict = dict(), **kwargs) -> str:
        """Get the transform options.

        Returns:
            str: The transform options.
        """
        cron = params.get('cron', params.get('cron_expr', None))
        if cron and (cron.lower().strip() == 'none' or not is_valid_cron(cron)):
            cron = None
        if cron:
            return sl_cron_start_end_dates(cron, cls.get_logical_datetime(context, config, **kwargs), params.get('sl_schedule_format', sl_schedule_format))
        else:
            return ''
