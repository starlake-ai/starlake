from ai.starlake.common import sanitize_id, sort_crons_by_frequency, sl_cron_start_end_dates

from ai.starlake.dagster.starlake_dagster_job import StarlakeDagsterJob

from ai.starlake.orchestration import IStarlakeOrchestration, StarlakeSchedules, StarlakeSchedule, StarlakeDependencies, StarlakeDomain, StarlakeDependency, StarlakeDependencyType

from dagster import AssetKey, ScheduleDefinition, GraphDefinition, Definitions, DependencyDefinition, JobDefinition, In, InputMapping, Out, Output, OutputMapping, graph, op, DefaultScheduleStatus, MultiAssetSensorDefinition, MultiAssetSensorEvaluationContext, RunRequest, SkipReason, Nothing, ScheduleDefinition

from dagster._core.definitions.input import InputDefinition

from dagster._core.definitions import NodeDefinition

from typing import Generic, List, Set, TypeVar, Union

U = TypeVar("U", bound=StarlakeDagsterJob)

class StarlakeDagsterOrchestration(Generic[U], IStarlakeOrchestration[Definitions, NodeDefinition]):
    def __init__(self, filename: str, module_name: str, job: U, **kwargs) -> None:
        """Overrides IStarlakeOrchestration.__init__()
        Args:
            filename (str): The filename of the orchestration.
            module_name (str): The module name of the orchestration.
            job (U): The job to orchestrate."""
        super().__init__(filename, module_name, job, **kwargs) 

    def sl_generate_scheduled_tables(self, schedules: StarlakeSchedules, **kwargs) -> Union[Definitions, List[Definitions]]:
        """Generate the Starlake dags that will orchestrate the load of the specified domains.

        Args:
            schedules (StarlakeSchedules): The required schedules
        
        Returns:
            Union[Definitions, List[Definitions]]: The generated dagster Definitions.
        """
        sl_job = self.job
        options: dict = self.options
        spark_config=self.spark_config

        crons = []

        pre_tasks = sl_job.pre_tasks()

        start = sl_job.dummy_op(task_id="start", ins={"start": In(str)} if pre_tasks else {})

        def load_domain(domain: StarlakeDomain, schedule: StarlakeSchedule) -> GraphDefinition:
            cron: Union[str, None] = schedule.cron

            if cron and len(schedules) > 1:
                schedule_name = schedule.name
            else:
                schedule_name = None

            if schedule_name:
                name=f"{domain.name}_{schedule_name}"
            else:
                name=f"{domain.name}"

            tables = [table.name for table in domain.tables]

            ins = {"domain": In(str)}

            op_tables = [sl_job.sl_load(task_id=None, domain=domain.name, table=table, ins=ins, cron=cron) for table in tables]

            ld_end = sl_job.dummy_op(task_id=f"{name}_load_ended", ins={f"{op_table._name}": In(str) for op_table in op_tables}, out="domain_loaded")

            ld_end_dependencies = dict()

            for op_table in op_tables:
                ld_end_dependencies[f"{op_table._name}"] = DependencyDefinition(op_table._name, 'result')

            ld_dependencies = {
                ld_end._name: ld_end_dependencies
            }

            ld_input_mappings=[
                InputMapping(
                    graph_input_name="domain", 
                    mapped_node_name=f"{op_table._name}",
                    mapped_node_input_name="domain",
                )
                for op_table in op_tables
            ]

            ld_output_mappings=[
                OutputMapping(
                    graph_output_name="domain_loaded",
                    mapped_node_name=f"{ld_end._name}",
                    mapped_node_output_name="domain_loaded",
                )
            ]

            ld = GraphDefinition(
                name=f"{name}_load",
                node_defs=op_tables + [ld_end],
                dependencies=ld_dependencies,
                input_mappings=ld_input_mappings,
                output_mappings=ld_output_mappings,
            )

            pld = sl_job.sl_pre_load(domain=domain.name, tables=set(tables), cron=cron, schedule=schedule_name)

            @op(
                name=f"{name}_load_result",
                ins={"inputs": In()},
                out={"result": Out(str)},
            )
            def load_domain_result(context, inputs):
                context.log.info(f"inputs: {inputs}")
                yield Output(str(inputs), "result")

            @graph(
                name=name,
                input_defs=[InputDefinition(name="domain", dagster_type=str)],
            )
            def domain_graph(domain):
                if pld:
                    load_domain, skip = pld(domain)
                    return load_domain_result([ld(load_domain), skip])
                else:
                    return ld(domain)

            return domain_graph

        def load_domains(schedule: StarlakeSchedule) -> GraphDefinition:
            cron = schedule.cron
            schedule_name = None
            if cron:
                crons.append(ScheduleDefinition(job_name = job_name(schedule), cron_schedule = cron, default_status=DefaultScheduleStatus.RUNNING))
                if len(schedules) > 1:
                    schedule_name = schedule.name

            if schedule_name:
                task_id=f"end_{schedule_name}"
            else:
                task_id="end"

            dependencies = dict()

            nodes = [start]

            if pre_tasks and pre_tasks.output_dict.keys().__len__() > 0:
                result = list(pre_tasks.output_dict.keys())[0]
                if result:
                    dependencies[start._name] = {
                        'start': DependencyDefinition(pre_tasks._name, result)
                    }
                    nodes.append(pre_tasks)

            node_defs = [load_domain(domain, schedule) for domain in schedule.domains]

            ins = dict()

            end_dependencies = dict()

            for node_def in node_defs:
                nodes.append(node_def)
                dependencies[node_def._name] = {
                    'domain': DependencyDefinition(start._name, 'result')
                }
                result = f"{node_def._name}_result"
                ins[result] = In(dagster_type=str)
                end_dependencies[result] = DependencyDefinition(node_def._name, 'result')

            end = sl_job.dummy_op(task_id=task_id, ins=ins, assets=[AssetKey(sl_job.sl_dataset(job_name(schedule), cron=cron))])
            nodes.append(end)
            dependencies[end._name] = end_dependencies

            post_tasks = sl_job.post_tasks(ins = {"start": In(str)})
            if post_tasks and post_tasks.input_dict.keys().__len__() > 0:
                input = list(post_tasks.input_dict.keys())[0]
                if input:
                    dependencies[post_tasks._name] = {
                        input: DependencyDefinition(end._name, 'result')
                    }
                    nodes.append(post_tasks)

            return GraphDefinition(
                name=f"schedule_{schedule.name}" if len(schedules) > 1 else 'schedule',
                node_defs=nodes,
                dependencies=dependencies,
            )

        def job_name(schedule: StarlakeSchedule) -> str:
            job_name = self.caller_filename.replace(".py", "").replace(".pyc", "").lower()
            return (f"{job_name}_{schedule.name}" if len(schedules) > 1 else job_name)

        def generate_job(schedule: StarlakeSchedule) -> JobDefinition:
            return JobDefinition(
                name=job_name(schedule),
                description=self.caller_globals.get('description', ""),
                graph_def=load_domains(schedule),
            )

        return Definitions(
            jobs=[generate_job(schedule) for schedule in schedules],
            schedules=crons,
        )

    def sl_generate_scheduled_tasks(self, dependencies: StarlakeDependencies, **kwargs) -> Definitions:
        """Generate the Starlake dag that will orchestrate the specified tasks.

        Args:
            dependencies (StarlakeDependencies): The required dependencies
        
        Returns:
            Definitions: The generated dagster Definitions.
        """

        sl_job = self.job
        options: dict = self.options
        spark_config=self.spark_config

        cron: str = self.caller_globals.get('cron', None)
        _cron = None if cron is None or cron.lower().strip() == "none" else cron

        job_name = self.caller_filename.replace(".py", "").replace(".pyc", "").lower()

        # if you want to load dependencies, set load_dependencies to True in the options
        load_dependencies: bool = sl_job.get_context_var(var_name='load_dependencies', default_value='False', options=options).lower() == 'true'

        sensor = None

        assets: Set[str] = set()

        cronAssets: dict = dict()

        all_dependencies: set = set()

        _filtered_assets: Set[str] = set(self.caller_globals.get('filtered_assets', []))

        first_level_tasks: set = set()

        def load_task_dependencies(task: StarlakeDependency):
            if len(task.dependencies) > 0:
                for subtask in task.dependencies:
                    all_dependencies.add(subtask.name)
                    load_task_dependencies(subtask)

        for task in dependencies:
            task_id = task.name
            first_level_tasks.add(task_id)
            _filtered_assets.add(sanitize_id(task_id).lower())
            load_task_dependencies(task)

        # if you choose to not load the dependencies, a sensor will be created to check if the dependencies are met
        if not load_dependencies:

            def load_assets(task: StarlakeDependency):
                if len(task.dependencies) > 0:
                    for child in task.dependencies:
                        asset = sanitize_id(child.name).lower()
                        if asset not in assets and asset not in _filtered_assets:
                            childCron = None if child.cron == 'None' else child.cron
                            if childCron :
                                cronAsset = sl_job.sl_dataset(asset, cron=childCron)
                                assets.add(cronAsset)
                                cronAssets[cronAsset] = childCron
                            else :
                                assets.add(asset)

            for task in dependencies:
                load_assets(task)

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

            sensor = MultiAssetSensorDefinition(
                name = f'{job_name}_sensor',
                monitored_assets = list(map(lambda asset: AssetKey(asset), assets)),
                asset_materialization_fn = multi_asset_sensor_with_skip_reason,
                minimum_interval_seconds = 60,
                description = f"Sensor for {job_name}",
                job_name = job_name,
            )

        def compute_task_id(task: StarlakeDependency) -> str:
            task_name = task.name
            task_type = task.dependency_type
            task_id = sanitize_id(task_name)
            if task_type == StarlakeDependencyType.task:
                task_id = task_id + "_task"
            else:
                task_id = task_id + "_table"
            return task_id

        if _cron:
            cron_expr = _cron
        elif assets.__len__() == cronAssets.__len__() and set(cronAssets.values()).__len__() > 0:
            sorted_crons = sort_crons_by_frequency(
                set(cronAssets.values()), 
                period=sl_job.get_context_var(
                    var_name='cron_period_frequency', 
                    default_value='week', 
                    options=options
                )
            )
            cron_expr = sorted_crons[0][0]
        else:
            cron_expr = None

        def create_task(task_id: str, task_name: str, task_type: StarlakeDependencyType, ins: dict={"start": In(Nothing)}):
            spark_config_name=sl_job.get_context_var('spark_config_name', task_name.lower(), options)
            if task_type == StarlakeDependencyType.task:
                if cron_expr:
                    transform_options = sl_cron_start_end_dates(cron_expr) #FIXME using execution date from context
                else:
                    transform_options = None
                return sl_job.sl_transform(
                    task_id=task_id, 
                    transform_name=task_name,
                    transform_options=transform_options,
                    spark_config=spark_config(spark_config_name, **self.caller_globals.get('spark_properties', {})),
                    ins=ins,
                    cron=_cron
                )
            else:
                load_domain_and_table = task_name.split(".",1)
                domain = load_domain_and_table[0]
                table = load_domain_and_table[1]
                return sl_job.sl_load(
                    task_id=task_id, 
                    domain=domain, 
                    table=table,
                    spark_config=spark_config(spark_config_name, self.caller_globals.get('spark_properties', {})),
                    ins=ins,
                    cron=_cron
                )

        pre_tasks = sl_job.pre_tasks()

        start = sl_job.dummy_op(task_id="start", ins={"start": In(str)} if pre_tasks else {})

        def generate_node_for_task(task: StarlakeDependency, computed_dependencies: dict, nodes: List[NodeDefinition], snodes: set) -> NodeDefinition :
            task_name = task.name
            task_type = task.dependency_type
            task_id = compute_task_id(task)

            children = []
            if load_dependencies and len(task.dependencies) > 0: 
                children = task.dependencies
            else:
                for child in task.dependencies:
                    if child.name in first_level_tasks:
                        children.append(child)

            if children.__len__() > 0:

                parent_dependencies = dict()

                ins = dict()

                for child in task.dependencies:
                    child_task_id = compute_task_id(child)
                    if child_task_id not in snodes:
                        ins[child_task_id] = In(Nothing)
                        parent_dependencies[child_task_id] = DependencyDefinition(child_task_id, 'result')
                        nodes.append(generate_node_for_task(child, computed_dependencies, nodes, snodes))
                        snodes.add(child_task_id)

                computed_dependencies[task_id] = parent_dependencies

                parent = create_task(
                    task_id=task_id, 
                    task_name=task_name, 
                    task_type=task_type, 
                    ins=ins
                )
                nodes.append(parent)

                return parent

            else:
                node = create_task(
                    task_id=task_id, 
                    task_name=task_name, 
                    task_type=task_type
                )
                nodes.append(node)
                computed_dependencies[task_id] = {
                    'start': DependencyDefinition(start._name, 'result')
                }
                return node

        def generate_job():
            computed_dependencies = dict()
            nodes = [start]
            snodes = set()

            if pre_tasks and pre_tasks.output_dict.keys().__len__() > 0:
                result = list(pre_tasks.output_dict.keys())[0]
                if result:
                    computed_dependencies[start._name] = {
                        'start': DependencyDefinition(pre_tasks._name, result)
                    }
                    nodes.append(pre_tasks)

            ins = dict()
            end_dependencies = dict()
            for task in dependencies:
                if task.name not in all_dependencies:
                    node = generate_node_for_task(task, computed_dependencies, nodes, snodes)
                    ins[node._name] = In(Nothing)
                    end_dependencies[node._name] = DependencyDefinition(node._name, 'result')

            asset_events: List[AssetKey] = [AssetKey(sl_job.sl_dataset(job_name, cron=_cron))]
            if set(cronAssets.values()).__len__() > 1: # we have at least 2 distinct cron expressions
                # we sort the cron assets by frequency (most frequent first)
                sorted_assets = sort_crons_by_frequency(
                    set(cronAssets.values()), 
                    period=sl_job.get_context_var(
                        var_name='cron_period_frequency', 
                        default_value='week', 
                        options=options
                    )
                )
                # we exclude the most frequent cron asset
                least_frequent_crons = set([expr for expr, _ in sorted_assets[1:sorted_assets.__len__()]])
                for cronAsset, cron in cronAssets.items() :
                    # we republish the least frequent scheduled assets
                    if cron in least_frequent_crons:
                        asset_events.append(AssetKey(cronAsset))
            end = sl_job.dummy_op(task_id="end", ins=ins, assets=asset_events)
            nodes.append(end)
            computed_dependencies[end._name] = end_dependencies

            post_tasks = sl_job.post_tasks(ins={"start": In(Nothing)})
            if post_tasks and post_tasks.input_dict.keys().__len__() > 0:
                input = list(post_tasks.input_dict.keys())[0]
                if input:
                    computed_dependencies[post_tasks._name] = {
                        input: DependencyDefinition(end._name, 'result')
                    }
                    nodes.append(post_tasks)

            return JobDefinition(
                name=job_name,
                description=self.caller_globals.get('description', ""),
                graph_def=GraphDefinition(
                    name=job_name,
                    node_defs=nodes,
                    dependencies=computed_dependencies,
                ),
            )

        crons = []

        if _cron:
            crons.append(ScheduleDefinition(job_name = job_name, cron_schedule = _cron, default_status=DefaultScheduleStatus.RUNNING))

        return Definitions(
            jobs=[generate_job()],
            schedules=crons,
            sensors=[sensor] if sensor else [],
        )
