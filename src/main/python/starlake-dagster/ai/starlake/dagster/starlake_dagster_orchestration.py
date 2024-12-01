from ai.starlake.common import sanitize_id, sl_cron_start_end_dates

from ai.starlake.dagster.starlake_dagster_job import StarlakeDagsterJob

from ai.starlake.resource import StarlakeResource

from ai.starlake.orchestration import StarlakeOrchestration, StarlakeSchedule, StarlakeDependencies, StarlakePipeline, StarlakeTaskGroup

from dagster import AssetKey, ScheduleDefinition, GraphDefinition, Definitions, DependencyDefinition, JobDefinition, In, InputMapping,OutputMapping, DefaultScheduleStatus, MultiAssetSensorDefinition, MultiAssetSensorEvaluationContext, RunRequest, SkipReason, ScheduleDefinition, OpDefinition

from dagster._core.definitions.output import OutputDefinition

from dagster._core.definitions import NodeDefinition

from typing import Generic, List, Optional, Set, TypeVar

J = TypeVar("J", bound=StarlakeDagsterJob)

class StarlakeDagsterPipeline(Generic[J], StarlakePipeline[JobDefinition, NodeDefinition, AssetKey, J, GraphDefinition]):
    def __init__(self, sl_job: J, group_id: str, sl_pipeline_id: str, sl_schedule: Optional[StarlakeSchedule] = None, sl_schedule_name: Optional[str] = None, sl_dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> None:
        if not isinstance(sl_job, StarlakeDagsterJob):
            raise TypeError(f"Expected an instance of StarlakeDagsterJob, got {type(sl_job).__name__}")
        super().__init__(sl_job, sl_pipeline_id, sl_schedule, sl_schedule_name, sl_dependencies, **kwargs)

        self.group_id = group_id
        task_group: StarlakeTaskGroup[NodeDefinition, GraphDefinition] = self.sl_create_task_group(group_id=group_id)
        self.task_group = task_group

        # Dynamically bind pipeline methods to GraphDefinition
        for attr_name in dir(self):
            if (attr_name == '__enter__' or attr_name == '__exit__' or not attr_name.startswith('__')) and callable(getattr(self, attr_name)):
                setattr(self.task_group.group, attr_name, getattr(self, attr_name))

    def __enter__(self):
        return self.task_group.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):

        self.sl_print_pipeline()

        graph_inputs = dict()
        downstream_input_mappings_dict = dict()

        def update_graph_def(task_group: GraphDefinition) -> GraphDefinition:
            group_id = task_group.get_sl_group_id()
            upstream_dependencies: dict = task_group.get_sl_upstream_dependencies()
            downstream_dependencies: dict = task_group.get_sl_downstream_dependencies()
            upstream_keys = upstream_dependencies.keys()
            downstream_keys = downstream_dependencies.keys()
            roots = upstream_keys - downstream_keys
            leaves = downstream_keys - upstream_keys
            tasks_dict: dict = task_group.get_sl_tasks_dict()
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

            def update_downstream_input_mappings_and_inputs(downstream: str, downstream_node: NodeDefinition, result: str, output: OutputDefinition):
                """
                Updates the input mappings and inputs of a downstream node.

                Args:
                    downstream (str): The name of the downstream node.  
                    downstream_node (OpDefinition): The downstream node to update.
                    result (str): The name of the result of the upstream node.
                    output (OutputDefinition): The output definition of the upstream node.
                """
                if isinstance(downstream_node, GraphDefinition):
                    downstream_input_mappings = downstream_input_mappings_dict.get(downstream, [])
                    downstream_roots: List[str] = downstream_node.get_sl_roots()
                    downstream_tasks_dicts = downstream_node.get_sl_tasks_dict()
                    for downstream_root in downstream_roots:
                        downstream_root_node = downstream_tasks_dicts.get(downstream_root, None)
                        if not downstream_root_node:
                            raise ValueError(f"Task {downstream_root} not found in task group {downstream}")
                        if len(downstream_root_node._input_defs) > 0:
                            downstream_input_mappings.append(
                                InputMapping(
                                    graph_input_name=result,
                                    mapped_node_name=downstream_root,
                                    mapped_node_input_name=downstream_root_node._input_defs[0].name,
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

            def get_leaves_nodes(nodes: list) -> list:
                tmp = []
                for node in nodes:
                    if isinstance(node, GraphDefinition):
                        leaves: list = get_leaves_nodes(node.get_sl_leaves_tasks()) 
                        tmp.extend(leaves)
                    else:
                        tmp.append(node)
                return tmp

            def walk_downstream(root_key: str):
                if root_key in walked_downstream:
                    return
                root: NodeDefinition = tasks_dict.get(root_key, None)
                if not root:
                    raise ValueError(f"Task {root_key} not found in task group {task_group.group_id}")
                if root_key in upstream_keys:
                    for downstream in upstream_dependencies[root_key]:
                        downstream_node = tasks_dict.get(downstream, None)
                        if not downstream_node:
                            raise ValueError(f"Task {downstream} not found in task group {task_group.group_id}")
                        inputs = graph_inputs.get(downstream, {})
                        dependencies = graph_dependencies.get(downstream, {})
                        for output_key, output in root._output_dict.items():
                            result = f"{root_key}_{output_key}"
                            inputs[result] = In(dagster_type=output._dagster_type)
                            graph_inputs[downstream] = inputs
                            dependencies[result] = DependencyDefinition(root_key, output_key)
                            graph_dependencies[downstream] = dependencies
                            update_downstream_input_mappings_and_inputs(downstream, downstream_node, result, output)

                        walk_downstream(downstream)

                elif root_key in downstream_keys:
                    for upstream in downstream_dependencies[root_key]:
                        upstream_node = tasks_dict.get(upstream, None)
                        if not upstream_node:
                            raise ValueError(f"Task {upstream} not found in task group {task_group.group_id}")
                        inputs = graph_inputs.get(root_key, {})
                        dependencies = graph_dependencies.get(root_key, {})
                        if isinstance(upstream_node, GraphDefinition):
                            upstream_leaves = get_leaves_nodes(upstream_node.get_sl_leaves_tasks())
                            for upstream_leaf in upstream_leaves:
                                for output_key, output in upstream_leaf._output_dict.items():
                                    result = f"{upstream_leaf.sl_task_id}_{output_key}"
                                    inputs[result] = In(dagster_type=output._dagster_type)
                                    graph_inputs[root_key] = inputs
                                    dependencies[result] = DependencyDefinition(upstream, result)
                            graph_dependencies[root_key] = dependencies
                        else:
                            for output_key, output in upstream_node._output_dict.items():
                                result = f"{upstream}_{output_key}"
                                dependencies[result] = DependencyDefinition(upstream, output_key)
                                graph_dependencies[root_key] = dependencies


                if root_key in roots:
                    if len(root._input_defs) > 0:
                        input_mappings.append(
                            InputMapping(
                                graph_input_name=group_id, 
                                mapped_node_name=root_key,
                                mapped_node_input_name=root._input_defs[0].name,
                            )
                        )

                if root_key in leaves:
                    if len(root._output_defs) > 0:
                        output_mappings.append(
                            OutputMapping(
                                graph_output_name=f'{root_key}_result',
                                mapped_node_name=root_key,
                                mapped_node_output_name=root._output_defs[0].name,
                            )
                        )
                
                walked_downstream.add(root_key)

            for root_key in roots:
                walk_downstream(root_key)

            for task in tasks_dict.values():
                if isinstance(task, GraphDefinition):
                    tasks_dict[task.sl_task_id] = update_graph_def(task)
 
            nodes = [copy_node_with_new_inputs(tasks_dict[key], graph_inputs.get(key, None)) for key in tasks_dict.keys()]

            input_mappings = downstream_input_mappings_dict.get(group_id)

#            print(f'Group: {group_id},{graph_dependencies} -> {roots},{input_mappings} -> {leaves},{output_mappings}')

            return GraphDefinition(
                name=group_id,
                node_defs=nodes,
                dependencies=graph_dependencies,
                input_mappings=input_mappings,
                output_mappings=output_mappings,
            )

        self.job_definition = JobDefinition(
            name=self.sl_pipeline_id,
            description=self.sl_job.caller_globals.get('description', ""),
            graph_def=update_graph_def(self.sl_task_groups_dict[self.group_id]),
        )

        self.sl_task_groups_dict.clear()

        return False

    def sl_create_internal_task_group(self, group_id: str, **kwargs) -> GraphDefinition:
        return GraphDefinition(name=group_id)

    def is_sl_task_group(self, task: NodeDefinition) -> bool:
        return isinstance(task, GraphDefinition)

    def get_sl_transform_options(self, cron_expr: Optional[str] = None) -> Optional[str]:
        if cron_expr:
            return sl_cron_start_end_dates(cron_expr) #FIXME using execution date from context
        return None

    @classmethod
    def to_event(cls, resource: StarlakeResource, source: Optional[str] = None) -> AssetKey:
        return AssetKey(resource.url)

class StarlakeDagsterTaskGroup(StarlakeTaskGroup[NodeDefinition, GraphDefinition]):
    def __init__(self, group_id: str, group: GraphDefinition, **kwargs) -> None:
        super().__init__(group_id, group, **kwargs)


class StarlakeDagsterOrchestration(Generic[J], StarlakeOrchestration[Definitions, NodeDefinition, AssetKey, J]):
    def __init__(self, job: J, **kwargs) -> None:
        """Overrides IStarlakeOrchestration.__init__()
        Args:
            filename (str): The filename of the orchestration.
            module_name (str): The module name of the orchestration.
            job (J): The job to orchestrate."""
        super().__init__(job, **kwargs) 
        self.pipelines: List[StarlakeDagsterPipeline] = []
        self.definitions: Definitions = Definitions()

        # Dynamically bind pipeline methods to GraphDefinition
        for attr_name in dir(self):
            if (attr_name == '__enter__' or attr_name == '__exit__' or not attr_name.startswith('__')) and callable(getattr(self, attr_name)):
                setattr(self.definitions, attr_name, getattr(self, attr_name))

    def get_sl_definitions(self) -> Definitions: 
        return self.definitions

    def __enter__(self):
        return self.definitions

    def __exit__(self, exc_type, exc_value, traceback):
        def multi_asset_sensor_with_skip_reason(context: MultiAssetSensorEvaluationContext):
            asset_events = context.latest_materialization_records_by_key()
            if all(asset_events.values()):
                context.advance_all_cursors()
                return RunRequest()
            elif any(asset_events.values()):
                materialized_asset_key_strs = [
                    key.to_user_string() for key, value in asset_events.items() if value
                ]
                not_materialized_asset_key_strs = [
                    key.to_user_string() for key, value in asset_events.items() if not value
                ]
                return SkipReason(
                    f"Observed materializations for {materialized_asset_key_strs}, "
                    f"but not for {not_materialized_asset_key_strs}"
                )
            else:
                return SkipReason("No materializations observed")

        sensors = []
        crons = []

        for pipeline in self.pipelines:

            pipeline_id = pipeline.get_sl_pipeline_id()

            resources = pipeline.get_sl_resources()

            cron = pipeline.get_sl_cron()

            if resources:
                assets = [pipeline.to_event(resource) for resource in resources]
                sensors.append(
                    MultiAssetSensorDefinition(
                        name = f'{pipeline_id}_sensor',
                        monitored_assets = assets,
                        asset_materialization_fn = multi_asset_sensor_with_skip_reason,
                        minimum_interval_seconds = 60,
                        description = f"Sensor for {pipeline_id}",
                        job_name = pipeline_id,
                    )
                )
            elif cron:
                crons.append(ScheduleDefinition(job_name = pipeline_id, cron_schedule = cron, default_status=DefaultScheduleStatus.RUNNING))

        defs = Definitions(
            jobs=[pipeline.job_definition for pipeline in self.pipelines],
            sensors=sensors,
            schedules=crons,
        )

        import sys

        module = sys.modules[self.job.caller_module_name]

        # Dynamically bind dagster definitions to module
        self.definitions = defs
        setattr(module, 'defs', defs)

        return False

    def sl_create_schedule_pipeline(self, sl_schedule: StarlakeSchedule, nb_schedules: int = 1, **kwargs) -> StarlakeDagsterPipeline[J]:
        """Create the Starlake pipeline that will generate the dag to orchestrate the load of the specified domains.

        Args:
            schedule (StarlakeSchedule): The required schedule
        
        Returns:
            StarlakePipeline: The pipeline.
        """
        sl_job = self.job

        pipeline_name = sl_job.caller_filename.replace(".py", "").replace(".pyc", "").lower()

        if nb_schedules > 1:
            sl_pipeline_id = f"{pipeline_name}_{sl_schedule.name}"
            sl_schedule_name = sl_schedule.name
        else:
            sl_pipeline_id = pipeline_name
            sl_schedule_name = None

        pipeline = StarlakeDagsterPipeline(
            sl_job, 
            sanitize_id(f"{sl_pipeline_id}_{sl_schedule.name}") if nb_schedules > 1 else sl_pipeline_id,
            sl_pipeline_id, 
            sl_schedule, 
            sl_schedule_name,
        )

        self.pipelines.append(pipeline)

        return pipeline

    def sl_create_dependencies_pipeline(self, dependencies: StarlakeDependencies, **kwargs) -> StarlakeDagsterPipeline[J]:
        """Create the Starlake pipeline that will generate the dag to orchestrate the specified tasks.

        Args:
            dependencies (StarlakeDependencies): The required dependencies
        
        Returns:
            StarlakePipeline: The pipeline.
        """
        sl_job = self.job

        sl_pipeline_id = sl_job.caller_filename.replace(".py", "").replace(".pyc", "").lower()

        pipeline = StarlakeDagsterPipeline(
            sl_job, 
            sl_pipeline_id,
            sl_pipeline_id = sl_pipeline_id, 
            sl_dependencies = dependencies, 
            **kwargs
        )

        self.pipelines.append(pipeline)

        return pipeline
