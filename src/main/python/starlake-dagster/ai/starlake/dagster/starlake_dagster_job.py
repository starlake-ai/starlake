from typing import List, Optional, Union

from ai.starlake.job import StarlakePreLoadStrategy, IStarlakeJob, StarlakeSparkConfig, StarlakeOptions, StarlakeOrchestrator, TaskType

from ai.starlake.common import is_valid_cron, sl_cron_start_end_dates, sl_timestamp_format, StarlakeParameters

from ai.starlake.dataset import StarlakeDataset, AbstractEvent

from dagster import AssetKey, Output, Out, op, AssetMaterialization, OpExecutionContext

from dagster._core.definitions import NodeDefinition

from dagster._core.definitions.partition import PARTITION_NAME_TAG

from datetime import datetime

import pytz

class DagsterDataset(AbstractEvent[AssetKey]):
    @classmethod
    def to_event(cls, dataset: StarlakeDataset, source: Optional[str] = None) -> AssetKey:
        return AssetKey(dataset.uri)

class StarlakeDagsterJob(IStarlakeJob[NodeDefinition, AssetKey], StarlakeOptions, DagsterDataset):
    def __init__(self, filename: str=None, module_name: str=None, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, options: dict=None, **kwargs) -> None:
        super().__init__(filename=filename, module_name=module_name, pre_load_strategy=pre_load_strategy, options=options, **kwargs)
        import sys
        module = sys.modules.get(module_name) if module_name else None
        if module and hasattr(module, '__file__'):
            import os
            file_path = module.__file__
            stat = os.stat(file_path)
            default_start_date = datetime.fromtimestamp(stat.st_mtime, tz=pytz.timezone('UTC')).strftime('%Y-%m-%d')
        else:
            default_start_date = datetime.now().strftime('%Y-%m-%d')
        sd = __class__.get_context_var(var_name='start_date', default_value=default_start_date, options=self.options)
        import re
        pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        if pattern.fullmatch(sd):
            start_date = datetime.strptime(sd, "%Y-%m-%d").astimezone(pytz.timezone('UTC'))
        else:
            raise ValueError(f"Invalid start date: {sd}")
        self.__start_date = start_date
        print(f"Using start date: {start_date}")
        self.__optional_dataset_enabled = str(__class__.get_context_var(var_name='optional_dataset_enabled', default_value="false", options=self.options)).strip().lower() == "true"
        self.__data_cycle_enabled = str(__class__.get_context_var(var_name='data_cycle_enabled', default_value="false", options=self.options)).strip().lower() == "true"
        self.data_cycle = str(__class__.get_context_var(var_name='data_cycle', default_value="none", options=self.options))
        self.__beyond_data_cycle_enabled = str(__class__.get_context_var(var_name='beyond_data_cycle_enabled', default_value="true", options=self.options)).strip().lower() == "true"

    @property
    def start_date(self) -> datetime:
        return self.__start_date

    @property
    def data_cycle_enabled(self) -> bool:
        """whether the data cycle is enabled or not."""
        return self.__data_cycle_enabled

    @property
    def data_cycle(self) -> Optional[str]:
        """Get the data cycle value."""
        return self.__data_cycle

    @data_cycle.setter
    def data_cycle(self, value: Optional[str]) -> None:
        """Set the data cycle value."""
        if self.data_cycle_enabled and value:
            data_cycle = value.strip().lower()
            if data_cycle == "none":
                self.__data_cycle = None
            elif data_cycle == "hourly":
                self.__data_cycle = "0 * * * *"
            elif data_cycle == "daily":
                self.__data_cycle = "0 0 * * *"
            elif data_cycle == "weekly":
                self.__data_cycle = "0 0 * * 0"
            elif data_cycle == "monthly":
                self.__data_cycle = "0 0 1 * *"
            elif data_cycle == "yearly":
                self.__data_cycle = "0 0 1 1 *"
            elif is_valid_cron(data_cycle):
                self.__data_cycle = data_cycle
            else:
                raise ValueError(f"Invalid data cycle value: {data_cycle}")
        else:
            self.__data_cycle = None

    @property
    def optional_dataset_enabled(self) -> bool:
        """whether a dataset can be optional or not."""
        return self.__optional_dataset_enabled

    @property
    def beyond_data_cycle_enabled(self) -> bool:
        """whether the beyond data cycle feature is enabled or not."""
        return self.__beyond_data_cycle_enabled

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
            kwargs.update({'skip_or_start': True,})
            kwargs.update({'retries': 0,})

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
        def dummy(context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, **kwargs):
            yield Output(value=task_id, output_name=out)

            for asset in assets:
                yield AssetMaterialization(asset_key=asset.path, description=kwargs.get("description", f"Dummy op {task_id} execution succeeded"))

        return dummy

from dagster import Config

class DagsterLogicalDatetimeConfig(Config):
    logical_datetime: Optional[str]
    previous_logical_datetime: Optional[str] = None
    dry_run: bool = False

class StarlakeDagsterUtils:

    @classmethod
    def quote_datetime(cls, date_str: Optional[str]) -> Optional[str]:
        """Quote the datetime string.
        Args:
            date_str (str): The datetime string to quote.
        Returns:
            str: The quoted datetime string.
        """
        if not date_str:
            return None
        return date_str.replace(' ', 'T').replace(':', '.').replace('+', '_')

    @classmethod
    def unquote_datetime(cls, date_str: Optional[str]) -> Optional[str]:
        """Unquote the datetime string.
        Args:
            date_str (str): The datetime string to unquote.
        Returns:
            str: The unquoted datetime string.
        """
        if not date_str:
            return None
        return date_str.replace('T', ' ').replace('.', ':').replace('_', '+')

    @classmethod
    def get_logical_datetime(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, **kwargs) -> datetime:
        """Get the logical datetime.
        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.

        If the partition key is set, it will be used to determine the logical datetime.
        If the partition key is not set, it will use the logical_datetime from the config.
        If neither is set, it will use the launch time of the run or the current time if the launch time is not available.

        Returns:
            datetime: The logical datetime.
        """
        context.log.info(f"config -> {config}")
        try:
            partition_key = context.partition_key
        except Exception as e:
            context.log.warning(e)
            partition_key = context.get_tag(PARTITION_NAME_TAG) or context.get_tag('logical_datetime')
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
    def get_asset(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, dataset: Union[StarlakeDataset, str], **kwargs) -> AssetKey:
        """Get the asset.

        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            dataset (Union[StarlakeDataset, str): The dataset.

        Returns:
            AssetKey: The asset.
        """
        if isinstance(dataset, str):
            return AssetKey(dataset)
        return AssetKey(dataset.refresh(cls.get_logical_datetime(context, config, **kwargs)).url)

    @classmethod
    def get_materialization(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, dataset: Union[StarlakeDataset, str], **kwargs) -> AssetMaterialization:
        """Get the asset materialization.

        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            dataset (Union[StarlakeDataset, str]): The dataset.

        Returns:
            AssetMaterialization: The asset materialization.
        """
        from dagster._core.definitions.metadata import MetadataValue
        logical_datetime = cls.get_logical_datetime(context, config, **kwargs)
        partition_key = logical_datetime.strftime(sl_timestamp_format)
        if isinstance(dataset, str):
            metadata = {
                StarlakeParameters.URI_PARAMETER.value: dataset,
                StarlakeParameters.CRON_PARAMETER.value: None,
                StarlakeParameters.FRESHNESS_PARAMETER.value: MetadataValue.int(0),
            }
            asset_key = AssetKey(dataset)
        else:
            metadata = {
                StarlakeParameters.URI_PARAMETER.value: dataset.uri,
                StarlakeParameters.CRON_PARAMETER.value: dataset.cron,
                StarlakeParameters.FRESHNESS_PARAMETER.value: MetadataValue.int(dataset.freshness),
            }
            asset_key = AssetKey(dataset.uri)
        metadata.update({
            StarlakeParameters.SCHEDULED_DATE_PARAMETER.value: MetadataValue.timestamp(logical_datetime),
            StarlakeParameters.DRY_RUN_PARAMETER.value: MetadataValue.bool(config.dry_run),
        })
        tags = kwargs.get("tags", {})
        partition = cls.quote_datetime(partition_key)
        tags.update({
            StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value: partition,
            PARTITION_NAME_TAG: partition,
        })
        if config.previous_logical_datetime:
            tags[StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value] = cls.quote_datetime(config.previous_logical_datetime)
        return AssetMaterialization(
            asset_key=asset_key, 
            description=kwargs.get("description", f"Asset {asset_key.to_user_string()} materialized"),
            metadata=metadata,
            partition=partition_key,
            tags=tags
        )

    @classmethod
    def get_assets(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, datasets: List[StarlakeDataset], **kwargs) -> List[AssetKey]:
        """Get the assets.

        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            datasets (List[StarlakeDataset]): The datasets.

        Returns:
            List[AssetKey]: The assets.
        """
        return [cls.get_asset(context, config, dataset, **kwargs) for dataset in datasets]

    @classmethod
    def get_materializations(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, datasets: List[StarlakeDataset], **kwargs) -> List[AssetMaterialization]:
        """Get the asset materializations.

        Args:
            context (OpExecutionContext): The context.
            config (DagsterLogicalDatetimeConfig): The logical datetime config.
            datasets (List[StarlakeDataset]): The datasets.

        Returns:
            List[AssetMaterialization]: The asset materializations.
        """
        return [cls.get_materialization(context, config, dataset, **kwargs) for dataset in datasets]

    @classmethod
    def get_transform_options(cls, context: OpExecutionContext, config: DagsterLogicalDatetimeConfig, params: dict = dict(), **kwargs) -> str:
        """Get the transform options.

        Returns:
            str: The transform options.
        """
        previous_logical_datetime = config.previous_logical_datetime or context.get_tag('previous_logical_datetime')
        logical_datetime: datetime = cls.get_logical_datetime(context, config, **kwargs)
        if previous_logical_datetime and logical_datetime:
            return f"{StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value}='{cls.unquote_datetime(previous_logical_datetime)}',{StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value}='{logical_datetime.strftime(sl_timestamp_format)}'"
        cron = params.get(StarlakeParameters.CRON_PARAMETER.value, params.get('cron', params.get('cron_expr', None)))
        if cron and (cron.lower().strip() == 'none' or not is_valid_cron(cron)):
            cron = None
        if cron:
            return sl_cron_start_end_dates(cron, logical_datetime)
        else:
            return ''
