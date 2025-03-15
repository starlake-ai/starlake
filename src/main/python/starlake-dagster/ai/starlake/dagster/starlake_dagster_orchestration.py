from ai.starlake.dagster.starlake_dagster_job import StarlakeDagsterJob, DagsterDataset

from ai.starlake.job import StarlakeOrchestrator

from ai.starlake.orchestration import StarlakeSchedule, StarlakeDependencies

from dagster import AssetKey, ScheduleDefinition, GraphDefinition, Definitions, DependencyDefinition, JobDefinition, In, InputMapping,OutputMapping, DefaultScheduleStatus, MultiAssetSensorDefinition, MultiAssetSensorEvaluationContext, RunRequest, SkipReason, ScheduleDefinition, OpDefinition

from dagster._core.definitions.output import OutputDefinition

from dagster._core.definitions import NodeDefinition

from typing import Any, List, Optional, TypeVar, Union

J = TypeVar("J", bound=StarlakeDagsterJob)

from ai.starlake.orchestration import AbstractTask, AbstractTaskGroup, AbstractPipeline, AbstractOrchestration, AbstractDependency

class DagsterOrchestration(AbstractOrchestration[JobDefinition, OpDefinition, GraphDefinition, AssetKey]):
    def __init__(self, job: J, **kwargs) -> None:
        super().__init__(job, **kwargs)

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

            pipeline_id = pipeline.pipeline_id

            datasets = pipeline.datasets

            cron = pipeline.cron

            if datasets and len(datasets) > 0:
                def get_monitored_assets():
                    return [pipeline.to_event(dataset) for dataset in datasets]

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

        self.dag = JobDefinition(
            name=self.pipeline_id,
            description=self.job.caller_globals.get('description', ""),
            graph_def=update_graph_def(self),
        )
