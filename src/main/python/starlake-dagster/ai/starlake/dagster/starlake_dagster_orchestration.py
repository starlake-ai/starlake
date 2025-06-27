from ai.starlake.dagster.starlake_dagster_job import StarlakeDagsterJob, DagsterDataset, StarlakeDagsterUtils

from ai.starlake.common import sl_timestamp_format, sl_scheduled_date, is_valid_cron, get_cron_frequency, StarlakeParameters

from ai.starlake.dataset import StarlakeDataset, DatasetTriggeringStrategy

from ai.starlake.job import StarlakeOrchestrator, StarlakeExecutionMode

from ai.starlake.orchestration import StarlakeSchedule, StarlakeDependencies

from dagster import AssetKey, ScheduleDefinition, GraphDefinition, Definitions, DependencyDefinition, JobDefinition, In, InputMapping,OutputMapping, DefaultScheduleStatus, MultiAssetSensorDefinition, MultiAssetSensorEvaluationContext, RunRequest, SkipReason, ScheduleDefinition, OpDefinition, TimeWindowPartitionsDefinition, Partition, PartitionedConfig, EventLogRecord, AssetMaterialization, DagsterInstance, EventRecordsResult, AssetSpec

from dagster._core.definitions.output import OutputDefinition

from dagster._core.definitions import NodeDefinition

from dagster._core.definitions.metadata import TimestampMetadataValue, IntMetadataValue

from dagster._core.definitions.partition import PARTITION_NAME_TAG

from datetime import datetime
import pytz

from typing import Any, List, Optional, Sequence, Tuple, TypeVar, Union

J = TypeVar("J", bound=StarlakeDagsterJob)

from ai.starlake.orchestration import AbstractTask, AbstractTaskGroup, AbstractPipeline, AbstractOrchestration, AbstractDependency

class DagsterOrchestration(AbstractOrchestration[JobDefinition, OpDefinition, GraphDefinition, AssetKey]):
    def __init__(self, job: J, **kwargs) -> None:
        super().__init__(job, **kwargs)

    def __exit__(self, exc_type, exc_value, traceback):

        sensors = []
        crons = []

        for pipeline in self.pipelines:

            pipeline_id = pipeline.pipeline_id

            datasets = pipeline.datasets

            cron = pipeline.cron

            dg_pipeline: DagsterPipeline = pipeline

            def multi_asset_sensor_with_skip_reason(context: MultiAssetSensorEvaluationContext):
                asset_events = context.latest_materialization_records_by_key()
                if self.job.dataset_triggering_strategy == DatasetTriggeringStrategy.ANY:
                    events_checked = any(asset_events.values())
                else:
                    events_checked = all(asset_events.values())
                # If there are no asset events, we skip the run
                if events_checked:
                    # we first retrieve the materialized events
                    materialized_events = {key.to_user_string(): event for key, event in asset_events.items() if event}

                    # and convert them to datasets
                    materialized_datasets = {key: dg_pipeline.get_dataset_and_partition(event) for key, event in materialized_events.items()}

                    # we then retrieve the asset keys of the datasets that were materialized
                    materialized_asset_key_strs = materialized_datasets.keys()

                    # we also retrieve the asset keys of the datasets that were not materialized
                    not_materialized_asset_key_strs = [
                        key.to_user_string() for key, value in asset_events.items() if not value
                    ]

                    # then we retrieve the schedules of all the materialized datasets
                    materialized_schedules = {key: tuple[1] for key, tuple in materialized_datasets.items() if tuple[1] is not None}

                    if len(materialized_schedules.keys()) == 0:
                        if len(not_materialized_asset_key_strs) > 0:
                            # If some datasets not materialized, we skip the run
                            return SkipReason(
                                f"Observed materializations for {materialized_asset_key_strs}, "
                                f"but not for {not_materialized_asset_key_strs}"
                            )
                        else:
                            # If all datasets were materialized, we run the pipeline
                            return RunRequest()

                    # then we retrieve the materialized dataset with the most recent scheduled datetime
                    freshest_materialized_dataset: tuple = max(materialized_schedules.items(), key=lambda x: x[1], default=(None, None))
                    freshest_materialized_dataset_uri = freshest_materialized_dataset[0]
                    freshest_materialized_dataset_datetime: datetime = freshest_materialized_dataset[1]

                    # we then check the other datasets
                    missing_datasets = [
                        dataset for dataset in datasets if dataset.uri in not_materialized_asset_key_strs
                    ]
                    oldest_materialized_datasets = [
                        tuple[0] for tuple in materialized_datasets.values() if tuple[0].uri != freshest_materialized_dataset_uri
                    ]

                    datasets_to_check = missing_datasets + oldest_materialized_datasets

                    # if there are no datasets to check, we run the pipeline
                    if len(datasets_to_check) == 0:
                        return RunRequest()

                    # we check if all datasets are consistent with the most recent materialized dataset
                    t = dg_pipeline.check_datasets_freshness(freshest_materialized_dataset_datetime, datasets_to_check, materialized_schedules, context.instance)
                    checked: bool = t[0]
                    previous_partition: Optional[datetime] = t[1]
                    max_scheduled_date: Optional[datetime] = t[2]
                    if checked and previous_partition and max_scheduled_date:
                        # If all datasets are consistent with the most recent materialized dataset, we run the pipeline
                        print(f"All datasets are consistent with the most recent materialized dataset {freshest_materialized_dataset_datetime.strftime(sl_timestamp_format)}, we run the pipeline {pipeline_id}")
                        logical_datetime = max_scheduled_date.strftime(sl_timestamp_format)
                        previous_logical_datetime=previous_partition.strftime(sl_timestamp_format)
                        context.advance_cursor(asset_events)
                        return RunRequest(
                            run_config=dg_pipeline._ops_config(logical_datetime=logical_datetime, previous_logical_datetime=previous_logical_datetime),
                            partition_key=logical_datetime,
                            tags={
                                PARTITION_NAME_TAG: logical_datetime,
                                StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value: logical_datetime,
                                StarlakeParameters.DATA_INTERVAL_START_PARAMETER.value: previous_logical_datetime,
                            },
                        )
                    else:
                        all_materialized_assets = ",".join(materialized_asset_key_strs)
                        all_uris = ",".join([f"{dataset.uri}" for dataset in datasets_to_check])
                        # If any dataset is not consistent with the most recent materialized dataset, we skip the run
                        return SkipReason(
                            f"Observed materializations for {all_materialized_assets}, "
                            f"but some datasets {all_uris} are not consistent with {freshest_materialized_dataset_datetime.strftime(sl_timestamp_format)}"
                        )
                else:
                    return SkipReason("No materializations observed")

            if datasets and len(datasets) > 0:
                def get_monitored_assets():
                    return [dg_pipeline.to_event(dataset) for dataset in datasets]

                sensors.append(
                    MultiAssetSensorDefinition(
                        name = f'{pipeline_id}_sensor',
                        monitored_assets = get_monitored_assets(),
                        asset_materialization_fn = multi_asset_sensor_with_skip_reason,
                        minimum_interval_seconds = 60,
                        description = f"Sensor for {pipeline_id}",
                        job_name = pipeline_id,
                    )
                )
            elif cron:
                crons.append(ScheduleDefinition(job_name = pipeline_id, cron_schedule = cron, default_status=DefaultScheduleStatus.RUNNING))

        defs = Definitions(
            assets=[AssetSpec(asset.uri) for pipeline in self.pipelines for asset in pipeline.assets],
            jobs=[pipeline.dag for pipeline in self.pipelines],
            sensors=sensors,
            schedules=crons,
        )

        import sys

        module = sys.modules[self.job.caller_module_name]

        # Dynamically bind dagster definitions to module
        self.definitions = defs
        setattr(module, 'defs', defs)

        return super().__exit__(exc_type, exc_value, traceback)

    @classmethod
    def sl_orchestrator(cls) -> str:
        return StarlakeOrchestrator.DAGSTER

    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[JobDefinition, OpDefinition, GraphDefinition, AssetKey]:
        return DagsterPipeline(self.job, dag=None, schedule=schedule, dependencies=dependencies, orchestration=self, **kwargs)

    def sl_create_task_group(self, group_id: str, **kwargs) -> AbstractTaskGroup[GraphDefinition]:
        return AbstractTaskGroup(group_id, orchestration_cls = DagsterOrchestration, group = GraphDefinition(name=group_id), **kwargs)

    @classmethod
    def from_native(cls, native: Any) -> Optional[Union[AbstractTask[OpDefinition], AbstractTaskGroup[GraphDefinition]]]:
        """Create a task or task group from a native object.
        Args:
            native (Any): the native object.
        Returns:
            Optional[Union[AbstractTask[OpDefinition], AbstractTaskGroup[GraphDefinition]]]: the task or task group.
        """
        if isinstance(native, OpDefinition):
            return AbstractTask(native.name, native)
        elif isinstance(native, GraphDefinition):
            return AbstractTaskGroup(native.name, cls, native)
        else:
            return None

class DagsterPipeline(AbstractPipeline[JobDefinition, OpDefinition, GraphDefinition, AssetKey], DagsterDataset):
    def __init__(self, sl_job: J, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None,  orchestration: Optional[DagsterOrchestration] = None, **kwargs) -> None:
        super().__init__(sl_job, orchestration_cls = DagsterOrchestration, schedule = schedule, dependencies = dependencies, orchestration = orchestration, **kwargs)
        self.__start_date = sl_job.start_date

    @property
    def start_date(self) -> datetime:
        return self.__start_date

    def __exit__(self, exc_type, exc_value, traceback):
        super().__exit__(exc_type, exc_value, traceback)

        all_groups: dict = dict()

        def get_all_groups(node: AbstractDependency):
            if isinstance(node, AbstractTaskGroup):
                all_groups[node.group_id] = node
                for dependency in node.dependencies:
                    get_all_groups(dependency)

        get_all_groups(self)

        graph_inputs = dict()
        downstream_input_mappings_dict = dict()

        def update_graph_def(task_group: AbstractTaskGroup[GraphDefinition]) -> GraphDefinition:
            group_id = task_group.group_id
            upstream_dependencies: dict = task_group.upstream_dependencies
            downstream_dependencies: dict = task_group.downstream_dependencies
            upstream_keys = upstream_dependencies.keys()
            downstream_keys = downstream_dependencies.keys()
            roots = upstream_keys - downstream_keys
            leaves = downstream_keys - upstream_keys
            tasks_dict = task_group.dependencies_dict.copy()
            output_mappings = []
            graph_dependencies = dict()
            walked_downstream = set()

            def copy_node_with_new_inputs(existing_node: NodeDefinition, new_inputs: Optional[dict]):
                """
                Creates a copy of an existing OpDefinition with new input definitions.

                Args:
                    existing_node (OpDefinition): The existing node to copy.
                    new_inputs (dict): A dictionary where keys are input names, and values are `In` objects.

                Returns:
                    OpDefinition: A new OpDefinition with the modified inputs.
                """
                if new_inputs is None or not isinstance(existing_node, OpDefinition):
                    return existing_node
                # Create a new OpDefinition with the new inputs
                return OpDefinition(
                    compute_fn=existing_node.compute_fn,
                    name=existing_node.name,
                    ins=new_inputs,
                    outs=existing_node.outs,
                    description=existing_node.description,
                    config_schema=existing_node.config_schema,
                    required_resource_keys=existing_node.required_resource_keys,
                    tags=existing_node.tags,
                    version=existing_node.version,
                    retry_policy=existing_node.retry_policy,
                )

            def update_group_output_mappings():
                """
                Updates the output mappings of the current group.

                """
                for leaf in leaves:
                    task: AbstractDependency = tasks_dict.get(leaf, None)
                    if not task:
                        raise ValueError(f"Task {leaf} not found in task group {group_id}")
                    if isinstance(task, AbstractTaskGroup):
                        group: AbstractTaskGroup = all_groups.get(task.id, None) #FIXME
                        if not group:
                            raise ValueError(f"Task group {task.id} not found")
                        for leaf_task in get_leaves_nodes(group.leaves):
                            node = get_node_definition(leaf_task)
                            if len(node._output_defs) > 0:
                                result = f"{leaf_task.id}_result"
                                output_mappings.append(
                                    OutputMapping(
                                        graph_output_name=result,
                                        mapped_node_name=group.group_id,
                                        mapped_node_output_name=result, #FIXME node._output_defs[0].name,
                                    )
                                )
                    elif isinstance(task, AbstractTask):
                        node = get_node_definition(task)
                        if len(node._output_defs) > 0:
                            output_mappings.append(
                                OutputMapping(
                                    graph_output_name=f'{node._name}_result',
                                    mapped_node_name=node._name,
                                    mapped_node_output_name=node._output_defs[0].name,
                                )
                            )
                    else:
                        raise ValueError(f"Node: {type(task)}")

            def update_downstream_input_mappings_and_inputs(downstream: str, downstream_node: AbstractDependency, result: str, output: OutputDefinition):
                """
                Updates the input mappings and inputs of a downstream node.

                Args:
                    downstream (str): The name of the downstream node.  
                    downstream_node (AbstractDependency): The downstream node to update.
                    result (str): The name of the result of the upstream node.
                    output (OutputDefinition): The output definition of the upstream node.
                """
                if isinstance(downstream_node, AbstractTaskGroup):
                    downstream_input_mappings = downstream_input_mappings_dict.get(downstream, [])
                    downstream_roots: List[str] = downstream_node.roots_keys
                    downstream_tasks_dicts = downstream_node.dependencies_dict
                    for downstream_root in downstream_roots:
                        downstream_root_node = downstream_tasks_dicts.get(downstream_root, None)
                        if not downstream_root_node:
                            raise ValueError(f"Task {downstream_root} not found in task group {downstream}")
                        node = get_node_definition(downstream_root_node)
                        if len(node._input_defs) > 0:
                            downstream_input_mappings.append(
                                InputMapping(
                                    graph_input_name=result,
                                    mapped_node_name=downstream_root,
                                    mapped_node_input_name=node._input_defs[0].name,
                                )
                            )
                        else:
                            downstream_inputs = graph_inputs.get(downstream_root, {})
                            downstream_inputs[f'{downstream_root}_input'] = In(dagster_type=output._dagster_type)
                            graph_inputs[downstream_root] = downstream_inputs
                            downstream_input_mappings.append(
                                InputMapping(
                                    graph_input_name=result,
                                    mapped_node_name=downstream_root,
                                    mapped_node_input_name=f'{downstream_root}_input',
                                )
                            )
                        update_downstream_input_mappings_and_inputs(downstream_root, downstream_root_node, f'{downstream_root}_input', output)
                    downstream_input_mappings_dict[downstream] = downstream_input_mappings
                elif isinstance(downstream_node, NodeDefinition):
                    raise ValueError(f"Node {type(downstream_node)} not found in task group {downstream}")

            def get_leaves_nodes(nodes: list) -> list:
                tmp = []
                for node in nodes:
                    if isinstance(node, AbstractTaskGroup):
                        leaves: list = get_leaves_nodes(node.leaves) 
                        tmp.extend(leaves)
                    elif isinstance(node, AbstractDependency):
                        tmp.append(node)
                    else:
                        raise ValueError(f"Node: {type(node)}")
                return tmp

            def get_node_definition(node: AbstractDependency) -> NodeDefinition:
                if isinstance(node, AbstractTask):
                    return node.task
                elif isinstance(node, AbstractTaskGroup):
                    return node.group
                elif isinstance(node, NodeDefinition):
                    return node
                else:
                    print(f"Node: {type(node)}")
                    raise ValueError(f"Task {node} not found in task group {group_id}")

            def walk_downstream(root_key: str):
                if root_key in walked_downstream:
                    # print(f"Already walked {root_key}")
                    return
                root: AbstractDependency = tasks_dict.get(root_key, None)
                if not root:
                    raise ValueError(f"Task {root_key} not found in task group {group_id}")
                if root_key in upstream_keys:
                    for downstream in upstream_dependencies[root_key]:
                        downstream_node: AbstractDependency = tasks_dict.get(downstream, None)
                        if not downstream_node:
                            raise ValueError(f"Task {downstream} not found in task group {group_id}")
                        if isinstance(root, AbstractTaskGroup):
                            for leaf in get_leaves_nodes(root.leaves):
                                for output_key, output in get_node_definition(leaf)._output_dict.items():
                                    result = f"{leaf.id}_{output_key}"
                                    update_downstream_input_mappings_and_inputs(downstream, downstream_node, result, output)
                        else:
                            inputs = graph_inputs.get(downstream, {})
                            dependencies = graph_dependencies.get(downstream, {})
                            for output_key, output in get_node_definition(root)._output_dict.items():
                                result = f"{root_key}_{output_key}"
                                inputs[result] = In(dagster_type=output._dagster_type)
                                graph_inputs[downstream] = inputs
                                dependencies[result] = DependencyDefinition(root_key, output_key)
                                graph_dependencies[downstream] = dependencies
                                update_downstream_input_mappings_and_inputs(downstream, downstream_node, result, output)

                        walk_downstream(downstream)

                if root_key in downstream_keys:
                    for upstream in downstream_dependencies[root_key]:
                        upstream_node: AbstractDependency = tasks_dict.get(upstream, None)
                        if not upstream_node:
                            raise ValueError(f"Task {upstream} not found in task group {task_group.group_id}")
                        inputs = graph_inputs.get(root_key, {})
                        dependencies = graph_dependencies.get(root_key, {})
                        if isinstance(upstream_node, AbstractTaskGroup):
                            upstream_leaves = get_leaves_nodes(upstream_node.leaves)
                            for upstream_leaf in upstream_leaves:
                                for output_key, output in get_node_definition(upstream_leaf)._output_dict.items():
                                    result = f"{upstream_leaf.id}_{output_key}"
                                    inputs[result] = In(dagster_type=output._dagster_type)
                                    graph_inputs[root_key] = inputs
                                    dependencies[result] = DependencyDefinition(upstream, result)
                            graph_dependencies[root_key] = dependencies
                        else:
                            for output_key, output in get_node_definition(upstream_node)._output_dict.items():
                                result = f"{upstream}_{output_key}"
                                dependencies[result] = DependencyDefinition(upstream, output_key)
                                graph_dependencies[root_key] = dependencies
                
                walked_downstream.add(root_key)

            if not roots and len(upstream_keys) == 0 and len(downstream_keys) == 0:
                roots = tasks_dict.keys()
                leaves = roots

            for root_key in roots:
                walk_downstream(root_key)

            groups_dict = dict()

            for task in tasks_dict.values():
                if isinstance(task, AbstractTaskGroup):
                    groups_dict[task.id] = update_graph_def(task)

            update_group_output_mappings()

            tasks_dict.update(groups_dict)

            nodes = [copy_node_with_new_inputs(get_node_definition(tasks_dict[key]), graph_inputs.get(key, None)) for key in tasks_dict.keys()]

            input_mappings = downstream_input_mappings_dict.get(group_id)

            # print(f'Group: {group_id},{[node._name for node in nodes]},{graph_dependencies} -> {roots},{input_mappings} -> {leaves},{output_mappings}')

            return GraphDefinition(
                name=group_id,
                node_defs=nodes,
                dependencies=graph_dependencies,
                input_mappings=input_mappings,
                output_mappings=output_mappings,
            )

        self.graph = update_graph_def(self)

        cron = self.cron

        partition_config = None

        if cron:
            def fun(partition: Partition):
                value = partition.value
                if isinstance(value, datetime):
                    logical_datetime = value.strftime(sl_timestamp_format)
                else:
                    logical_datetime = str(value)

                return self._ops_config(logical_datetime)

            partition_config = PartitionedConfig(
                partitions_def=TimeWindowPartitionsDefinition(
                    cron_schedule = cron,
                    start=self.start_date,
                    fmt=sl_timestamp_format,
                    timezone='UTC',
                ),
                run_config_for_partition_fn=fun,
            )

        self.dag = JobDefinition(
            name=self.pipeline_id,
            description=self.job.caller_globals.get('description', ""),
            graph_def=self.graph,
            config=partition_config,
        )

    def _ops_config(self, logical_datetime: str, dry_run: bool = False, previous_logical_datetime: Optional[str] = None) -> dict:
        def walk(node: NodeDefinition, config: dict = dict()) -> dict:
            if isinstance(node, OpDefinition):
                config[node.name] = {'config': {'logical_datetime': logical_datetime, 'dry_run': dry_run, 'previous_logical_datetime': previous_logical_datetime}}
                return config
            elif isinstance(node, GraphDefinition):
                sub_config = dict()
                for sub_node in node.node_defs:
                    walk(sub_node, sub_config)
                config[node.name] = {'ops': sub_config}
                return config
            else:
                raise ValueError(f"Node {node}")
        
        ops_config = dict()
        for node in self.graph.node_defs:
            walk(node, ops_config)

        return {'ops': ops_config}

    def sl_transform_options(self, cron_expr: Optional[str] = None) -> Optional[str]:
        return None # should be implemented within dagster jobs sl_job method

    def get_previous_partition(self, instance: DagsterInstance, scheduled_date: datetime) -> Optional[datetime]:
        """Retrieves the most recent partition of a successful run for the pipeline before the specified scheduled date."""
        from dagster import RunsFilter, DagsterRunStatus

        runs = instance.get_runs(
            filters=RunsFilter(
                job_name=self.pipeline_id,
                statuses=[DagsterRunStatus.SUCCESS],
            ),
        )

        previous_partition = None

        if len(runs) > 0:
            matching_runs = []
            for run in runs:
                partition_key = run.tags.get(PARTITION_NAME_TAG, None) or run.tags.get(StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value, None) 
                if partition_key:
                    try:
                        from dateutil import parser
                        partition = partition_key.replace('T', ' ').replace('.', ':').replace('_', '+')
                        partition_date = parser.isoparse(partition).astimezone(pytz.timezone('UTC'))
                        print(f"Checking partition date: {partition_date} against scheduled date: {scheduled_date}")
                        if partition_date < scheduled_date:
                            matching_runs.append(partition_date)
                    except ValueError:
                        # La clé de partition n'est pas une date au format attendu
                        continue

            previous_partition = max(matching_runs, default=None)

        return previous_partition

    def get_materialization_partition(self, mat: AssetMaterialization) -> Optional[datetime]:
        """Extracts the partition from an asset materialization."""
        tags = mat.tags or {}
        partition_date = mat.partition or tags.get(PARTITION_NAME_TAG, None) or tags.get(StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value, None) or mat.metadata.get(StarlakeParameters.SCHEDULED_DATE_PARAMETER.value, None)
        if partition_date:
            if isinstance(partition_date, TimestampMetadataValue):
                return datetime.fromtimestamp(partition_date.value, pytz.timezone('UTC'))
            else:
                from dateutil import parser
                try:
                    partition = StarlakeDagsterUtils.unquote_datetime(partition_date)
                    return parser.isoparse(partition).astimezone(pytz.timezone('UTC'))
                except ValueError:
                    print(f"Invalid partition date {partition_date} for event {mat.asset_key.to_user_string()}")
                    return None
        else:
            print(f"No partition found for materialization {mat.asset_key.to_user_string()}")
            return None

    def get_event_partition(self, event: EventLogRecord) -> Optional[datetime]:
        """Extracts the partition from an event log record."""
        partition = event.partition_key
        if partition:
            from dateutil import parser
            try:
                partition = StarlakeDagsterUtils.unquote_datetime(partition)
                print(f"Partition {partition} found for event {event.asset_key.to_user_string()} with id {event.storage_id}")
                # Attempt to parse the partition key as a datetime
                return parser.isoparse(partition).astimezone(pytz.timezone('UTC'))
            except ValueError:
                print(f"Invalid partition key: {partition} for event {event.asset_key.to_user_string()} with id {event.storage_id}")
                partition = None
        if not partition:
            mat = event.asset_materialization
            if mat:
                return self.get_materialization_partition(mat)
        print(f"No partition found for event {event.asset_key.to_user_string()} with id {event.storage_id}")
        return None

    def get_cron(self, mat: AssetMaterialization) -> Optional[str]:
        """Extracts the cron from an asset materialization."""
        metadata = mat.metadata
        cron = metadata.get(StarlakeParameters.CRON_PARAMETER.value, None)
        if cron:
            cron_expr = str(cron.value)
            if is_valid_cron(cron_expr):
                return cron_expr
            else:
                print(f"Invalid cron expression: {cron_expr} in materialization {mat}")
        return None

    def get_freshness(self, mat: AssetMaterialization) -> Optional[int]:
        """Extracts the freshness from an asset materialization."""
        metadata = mat.metadata
        freshness = metadata.get(StarlakeParameters.FRESHNESS_PARAMETER.value, None)
        if freshness and isinstance(freshness, IntMetadataValue):
            return freshness.value
        return None

    def find_dataset_events(self, instance: DagsterInstance, asset_key: AssetKey, partitions: Optional[Sequence[str]] = None, limit: int = 100) -> Sequence[EventLogRecord]:
        """Retrieves the events for the specified dataset and optional partitions."""
        from dagster import AssetRecordsFilter

        materializations: EventRecordsResult = instance.fetch_materializations(
            records_filter=AssetRecordsFilter(
                asset_key=asset_key,
                asset_partitions=partitions,
            ),
            limit=limit,
        )

        return materializations.records

    def get_dataset_and_partition(self, event: EventLogRecord) -> Tuple[StarlakeDataset, Optional[datetime]]:
        """Extracts the dataset from an asset materialization."""
        uri = event.asset_key.to_user_string()
        dataset = next((dataset for dataset in self.datasets or [] if dataset.uri == uri), None)
        mat = event.asset_materialization
        if mat:
            parameters={key: value.value for key, value in mat.metadata.items()} if mat.metadata else {}
            if dataset and dataset.parameters:
                parameters.update(dataset.parameters)
            cron = parameters.get(StarlakeParameters.CRON_PARAMETER.value, None) or self.get_cron(mat) or (dataset.cron if dataset else None)
            freshness = parameters.get(StarlakeParameters.FRESHNESS_PARAMETER.value, None) or self.get_freshness(mat) or (dataset.freshness if dataset else 0)
            # If materialization, return a dataset with the name and start_time from the materialization
        else:
            if dataset:
                parameters = dataset.parameters
                cron = dataset.cron
                freshness = dataset.freshness
            else:
                parameters = None
                cron = None
                freshness = 0
        print(f"Dataset {uri} with parameters {parameters}, cron {cron}, freshness {freshness} for event {event.asset_key.to_user_string()} with id {event.storage_id}")
        return (
            StarlakeDataset(
                name=uri,
                sink=uri,
                parameters=parameters,
                cron=cron,
                freshness=freshness,
            ),
            self.get_event_partition(event)
        )

    def check_datasets_freshness(self, scheduled_date: datetime, datasets: List[StarlakeDataset], schedules: dict[str, datetime], instance: DagsterInstance, limit: int = 100) -> tuple:
        """Checks the freshness of the datasets and returns True if all datasets have been materialized arround the most recent one."""
        job: StarlakeDagsterJob = self.job
        missing_datasets: List[StarlakeDataset] = []
        max_scheduled_date = scheduled_date

        # Retrieves the most recent partition of a successful run for the pipeline before the scheduled date.
        previous_partition = self.get_previous_partition(instance, scheduled_date)

        if not previous_partition:
            start_date = job.start_date
            if start_date < scheduled_date:
                print(f"No previous successful runs found for {self.pipeline_id} around {scheduled_date}, using the start date {start_date.strftime(sl_timestamp_format)} defined for the pipeline")
                previous_partition = start_date
            else:
                print(f"No previous successful runs found for {self.pipeline_id} around {scheduled_date} and no start date set for the pipeline")
                return False

        print(f"Previous successful run for {self.pipeline_id} around {scheduled_date} is {previous_partition.strftime(sl_timestamp_format)}")

        data_cycle_freshness = None
        if job.data_cycle:
            # the freshness of the data cycle is the time delta between 2 iterations of its schedule
            data_cycle_freshness = get_cron_frequency(job.data_cycle)

        # Check each dataset for freshness
        from croniter import croniter
        from datetime import timedelta
        for dataset in datasets:
            original_cron = dataset.cron
            cron = original_cron or job.data_cycle
            scheduled = cron and is_valid_cron(cron)
            freshness = dataset.freshness
            optional = False
            beyond_data_cycle_allowed = False
            if data_cycle_freshness:
                original_scheduled = original_cron and is_valid_cron(original_cron)
                if job.optional_dataset_enabled:
                    # we check if the dataset is optional by comparing its freshness with that of the data cycle
                    # the freshness of a scheduled dataset is the time delta between 2 iterations of its schedule
                    # the freshness of a non scheduled dataset is defined by its freshness parameter
                    optional = (original_scheduled and abs(data_cycle_freshness.total_seconds()) < abs(get_cron_frequency(original_cron).total_seconds())) or (not original_scheduled and abs(data_cycle_freshness.total_seconds()) < freshness)
                if job.beyond_data_cycle_enabled:
                    # we check if the dataset scheduled date is allowed to be beyond the data cycle by comparing its freshness with that of the data cycle
                    beyond_data_cycle_allowed = (original_scheduled and abs(data_cycle_freshness.total_seconds()) < abs(get_cron_frequency(original_cron).total_seconds() + freshness)) or (not original_scheduled and abs(data_cycle_freshness.total_seconds()) < freshness)
            found = False
            if optional:
                print(f"Dataset {dataset.uri} is optional, we skip it")
                continue
            elif scheduled:
                iter = croniter(cron, scheduled_date)
                curr: datetime = iter.get_current(datetime)
                previous: datetime = iter.get_prev(datetime)
                next: datetime = croniter(cron, previous).get_next(datetime)
                if curr == next :
                    scheduled_date_to_check_max = curr
                else:
                    scheduled_date_to_check_max = previous
                scheduled_date_to_check_min: datetime = croniter(cron, scheduled_date_to_check_max).get_prev(datetime)
                if original_cron:
                    partitions = [
                        scheduled_date_to_check_min.strftime(sl_timestamp_format),
                        scheduled_date_to_check_max.strftime(sl_timestamp_format),
                    ]
                else:
                    partitions = None
                if not original_cron and previous_partition > scheduled_date_to_check_min:
                    scheduled_date_to_check_min = previous_partition
                if beyond_data_cycle_allowed:
                    scheduled_date_to_check_min = scheduled_date_to_check_min - timedelta(seconds=freshness)
                    scheduled_date_to_check_max = scheduled_date_to_check_max + timedelta(seconds=freshness)
                scheduled_datetime = schedules.get(dataset.uri, None)
                if scheduled_datetime:
                    # we check if the scheduled datetime is between the scheduled date to check min and max
                    if scheduled_date_to_check_min >= scheduled_datetime or scheduled_datetime > scheduled_date_to_check_max:
                        # we will check within the event logs
                        print(f"Triggering dataset {dataset.uri} with scheduled datetime {scheduled_datetime} not between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                    else:
                        found = True
                        print(f"Found trigerring dataset {dataset.uri} with scheduled datetime {scheduled_datetime} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                        if scheduled_datetime > max_scheduled_date:
                            max_scheduled_date = scheduled_datetime
                if not found:
                    events = self.find_dataset_events(instance, AssetKey(dataset.uri), partitions, limit)
                    if not events:
                        print(f"No dataset events for {dataset.uri} found between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                    else:
                        dataset_events = events
                        nb_events = len(events)
                        print(f"Found {nb_events} dataset event(s) for {dataset.uri} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                        i = 1
                        # we check the dataset events in reverse order
                        while i <= nb_events and not found:
                            event = dataset_events[nb_events - i]
                            mat = event.asset_materialization
                            scheduled_datetime = self.get_event_partition(event)
                            if scheduled_date_to_check_min >= scheduled_datetime or scheduled_datetime > scheduled_date_to_check_max:
                                print(f"Dataset {event.partition_key or event.storage_id} for {dataset.uri} with scheduled datetime {scheduled_datetime} not between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                i += 1
                            else:
                                found = True
                                print(f"Found dataset {dataset.uri} with materialization at {scheduled_datetime} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                if scheduled_datetime > max_scheduled_date:
                                    max_scheduled_date = scheduled_datetime
                                break
                    if not found:
                        missing_datasets.append(dataset)
            else:
                # we check if one dataset event at least has been published since the previous dag checked and around the scheduled date +- freshness in seconds - it should be the closest one
                scheduled_date_to_check_min = scheduled_date - timedelta(seconds=freshness)
                scheduled_date_to_check_max = scheduled_date + timedelta(seconds=freshness)
                events = self.find_dataset_events(instance, AssetKey(dataset.uri), limit=limit)
                if not events:
                    print(f"No dataset events for {dataset.uri} found between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                else:
                    dataset_events = events
                    nb_events = len(events)
                    print(f"Found {nb_events} dataset event(s) for {dataset.uri} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                    i = 1
                    # we check the dataset events in reverse order
                    while i <= nb_events and not found:
                        event = dataset_events[nb_events - i]
                        scheduled_datetime = self.get_event_partition(event)
                        if not scheduled_datetime:
                            i +=1
                        elif scheduled_datetime > previous_partition:
                            if scheduled_date_to_check_min > scheduled_datetime:
                                # TODO we should stop if all previous dataset events would be also before the scheduled date to check
                                # break;
                                i += 1
                            elif scheduled_datetime > scheduled_date_to_check_max:
                                i += 1
                            else:
                                found = True
                                print(f"Found dataset {dataset.uri} with materialization at {scheduled_datetime} between {scheduled_date_to_check_min} and {scheduled_date_to_check_max}")
                                if scheduled_datetime <= scheduled_date:
                                    # we stop because all previous dataset events would be also before the scheduled date but not closer than the current one
                                    break;
                        else:
                            #TODO we should stop if all previous dataset events would be also before the previous dag checked
                            #break;
                            i += 1
                if not found or not scheduled_datetime:
                    missing_datasets.append(dataset)
                    print(f"No dataset {dataset.uri} found since the previous partition {previous_partition} and around the scheduled date {scheduled_date} +- {freshness} in seconds")
                else:
                    print(f"Found dataset {dataset.uri} after the previous partition {previous_partition}  and  around the scheduled date {scheduled_date} +- {freshness} in seconds")
                    if scheduled_datetime > max_scheduled_date:
                        max_scheduled_date = scheduled_datetime

        # if all the required datasets have been found, we can run the dag
        checked = not missing_datasets
        if checked:
            print(f"All datasets checked: {', '.join([dataset.uri for dataset in datasets])}")
            print(f"Starlake start date will be set to {previous_partition}")
            print(f"Starlake end date will be set to {max_scheduled_date}")
            return (True, previous_partition, max_scheduled_date, [])
        else:
            print(f"Some datasets are missing: {', '.join([dataset.uri for dataset in missing_datasets])}")
            return (False, None, None, missing_datasets)

    def run(self, logical_date: Optional[str] = None, timeout: str = '120', mode: StarlakeExecutionMode = StarlakeExecutionMode.RUN, **kwargs) -> None:
        """Run the pipeline.
        Args:
            logical_date (Optional[str]): the logical date.
            timeout (str): the timeout in seconds.
            mode (StarlakeExecutionMode): the execution mode.
        """
        from dagster import DagsterInstance
        if not logical_date:
            #if a cron has been defined for the pipeline, use it to compute the logical date
            # otherwise use the current date
            cron_expr = self.computed_cron_expr
            if cron_expr and is_valid_cron(cron_expr):
                logical_date = sl_scheduled_date(cron_expr, datetime.now(pytz.utc), previous=False).strftime(sl_timestamp_format)
            else:
                print(f"No logical date provided and no cron defined for {self.pipeline_id}, using current date.")
                logical_date = datetime.now(pytz.utc).strftime(sl_timestamp_format)
        dry_run = mode == StarlakeExecutionMode.DRY_RUN
        run_config = self._ops_config(logical_date, dry_run)
        with DagsterInstance.ephemeral() as instance:
            # Run the job with the provided configuration
            print(f"Running {self.pipeline_id}...")
            print(f"Run config: {run_config}")
            job: JobDefinition = self.dag
            result = job.execute_in_process(
                run_config=run_config,
                instance=instance,
                tags={
                    PARTITION_NAME_TAG: logical_date,
                    StarlakeParameters.DATA_INTERVAL_END_PARAMETER.value: logical_date,
                },
            )

            for key, value in result.dagster_run.tags.items():
                print(f"Tag: {key} = {value}")

            for event in result.get_asset_materialization_events():
                mat = event.step_materialization_data.materialization
                if mat:
                    partition = self.get_materialization_partition(mat)
                    if partition:
                        partition = partition.strftime(sl_timestamp_format)
                        print(f"Found partition {partition} for event {event.asset_key.to_user_string()}")
                else:
                    partition = None

                if dry_run:
                    events = self.find_dataset_events(instance, event.asset_key, partitions=[partition] if partition else None, limit=1)
                    if events:
                        print(f"Found {len(events)} events for dataset {event.asset_key.to_user_string()} with partition {partition}")
                    else:
                        print(f"No events found for dataset {event.asset_key.to_user_string()} with partition {partition}")

            if dry_run:
                previous_partition = self.get_previous_partition(instance, datetime.strptime(logical_date, sl_timestamp_format).astimezone(pytz.timezone('UTC')))
                if previous_partition:
                    print(f"Previous partition: {previous_partition.strftime(sl_timestamp_format)}")
                else:
                    print("No previous partition found.")

            if dry_run and self.datasets:
                from dateutil import parser
                scheduled_date = parser.isoparse(logical_date).astimezone(pytz.timezone('UTC'))
                t = self.check_datasets_freshness(
                    scheduled_date,
                    self.datasets,
                    {key: scheduled_date for key in [dataset.uri for dataset in self.datasets]},
                    instance,
                    limit=1,
                )
                print(f"Datasets freshness check: {t[0]}")

            # Check execution success
            if result.success:
                print(f"Successful execution of {self.pipeline_id} !")
            else:
                for event in result.get_step_failure_events():
                    print(event.message)
                print(f"Execution of {self.pipeline_id} failed.")
