from __future__ import annotations

from ai.starlake.common import sanitize_id, sort_crons_by_frequency

from ai.starlake.job import StarlakeSparkConfig, IStarlakeJob

from ai.starlake.resource import StarlakeResource, StarlakeEvent

from ai.starlake.orchestration import StarlakeSchedules, StarlakeSchedule, StarlakeDomain, StarlakeDependencies, StarlakeDependencyType

from typing import Any, Generic, List, Optional, TypeVar, Union

U = TypeVar("U")

T = TypeVar("T")

E = TypeVar("E")

J = TypeVar("J", bound=IStarlakeJob[T, E])

class StarlakePipeline(Generic[U, T, E, J], StarlakeEvent[E]):
    def __init__(self, sl_job: J, sl_pipeline_id: str, sl_schedule: Optional[StarlakeSchedule] = None, sl_schedule_name: Optional[str] = None, sl_dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> None:
        self.sl_job = sl_job
        self.sl_pipeline_id = sl_pipeline_id
        self.sl_schedule = sl_schedule
        self.sl_schedule_name = sl_schedule_name
        self.sl_dependencies = sl_dependencies

        tags = self.get_sl_context_var(var_name='tags', default_value="").split()

        catchup: bool = False

        cron: Optional[str] = None

        load_dependencies: Optional[bool] = None
 
        resources: Optional[List[StarlakeResource]] = None

        events: Optional[List[E]] = None

        if sl_schedule is not None:
            cron = sl_schedule.cron
            for domain in sl_schedule.domains:
                tags.append(domain.name)

        elif sl_dependencies is not None:
            cron = sl_job.caller_globals.get('cron', None)

            if cron is not None:
                if cron.lower().strip() == "none":
                    cron = None
                elif not StarlakeSchedule.is_valid_cron(cron):
                    raise ValueError(f"Invalid cron expression: {cron}")

            catchup = cron is not None and self.get_sl_context_var(var_name='catchup', default_value='False').lower() == 'true'

            load_dependencies = self.get_sl_context_var(var_name='load_dependencies', default_value='False').lower() == 'true'

            filtered_datasets: Set[str] = set(sl_job.caller_globals.get('filtered_datasets', []))

            computed_schedule = sl_dependencies.get_schedule(
                cron=cron, 
                load_dependencies=load_dependencies,
                filtered_resources=filtered_datasets,
                sl_schedule_parameter_name=sl_job.sl_schedule_parameter_name,
                sl_schedule_format=sl_job.sl_schedule_format
            )

            if computed_schedule is not None:
                if isinstance(computed_schedule, str):
                    cron = computed_schedule
                elif isinstance(computed_schedule, list):
                    resources = computed_schedule

        self.sl_tags = tags

        self.sl_cron = cron

        self.sl_catchup = catchup

        self.sl_load_dependencies = load_dependencies

        self.sl_resources = resources

        if resources:
            events = list(map(lambda resource: self.to_event(resource=resource), resources))
        self.sl_events = events

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def get_sl_context_var(self, var_name: str, default_value: Any) -> Any:
        return self.sl_job.get_context_var(
            var_name=var_name, 
            default_value=default_value, 
            options=self.sl_job.options
        )

    def get_sl_pipeline_id(self) -> str:
        return self.sl_pipeline_id

    def get_sl_job(self) -> J:
        return self.sl_job

    def get_sl_caller_globals(self) -> dict:
        return self.sl_job.caller_globals

    def get_sl_spark_config(self, spark_config_name: str) -> StarlakeSparkConfig:
        return self.sl_job.get_spark_config(
            self.get_sl_context_var('spark_config_name', spark_config_name), 
            **self.get_sl_caller_globals().get('spark_properties', {})
        )

    def get_sl_schedule(self) -> Optional[StarlakeSchedule]:
        return self.sl_schedule

    def get_sl_schedule_name(self) -> Optional[str]:
        return self.sl_schedule_name

    def get_sl_dependencies(self) -> Optional[StarlakeDependencies]:
        return self.sl_dependencies

    def get_sl_cron(self) -> Optional[str]:
        return self.sl_cron

    def is_sl_catchup(self) -> bool:
        return self.sl_catchup

    def is_sl_load_dependencies(self) -> Optional[bool]:
        return self.sl_load_dependencies

    def get_sl_resources(self) -> Optional[List[StarlakeResource]]:
        return self.sl_resources

    def get_sl_transform_options(self, cron_expr: Optional[str] = None) -> Optional[str]:
        pass

    def sl_create_task_group(self, group_id: str, **kwargs) -> StarlakeTaskGroup[T]:
        pass

    def sl_dummy_op(self, task_id: str, **kwargs) -> T:
        return self.sl_job.dummy_op(task_id=task_id, **kwargs)

    def sl_start(self, **kwargs) -> T:
        pass

    def sl_pre_tasks(self, *args, **kwargs) -> T:
        return self.sl_job.pre_tasks()

    def sl_pre_load(self, domain: str, tables: Set[str], **kwargs) -> T:
        return self.sl_job.sl_pre_load(domain=domain, tables=tables, **kwargs)

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig, **kwargs) -> T:
        return self.sl_job.sl_load(
            task_id=task_id, 
            domain=domain, 
            table=table, 
            spark_config=spark_config, 
            **kwargs
        )

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str = None, spark_config: StarlakeSparkConfig = None, **kwargs) -> T:
        return self.sl_job.sl_transform(
            task_id=task_id, 
            transform_name=transform_name, 
            transform_options=transform_options, 
            spark_config=spark_config, 
            **kwargs
        )

    def sl_post_tasks(self, *args, **kwargs) -> T:
        return self.sl_job.post_tasks()

    def sl_end(self, output_resources: Optional[List[StarlakeResource]] = None, **kwargs) -> T:
        pass

    def sl_add_dependency(self, pipeline_upstream: T, pipeline_downstream: T, **kwargs) -> T:
        pass

    def sl_generate_pipeline(self, orchestration: StarlakeOrchestration[U, T], **kwargs) -> U:
        if self.sl_schedule:
            # generate the load pipeline

            with self as pipeline:
                schedule       = pipeline.get_sl_schedule()
                schedule_name  = pipeline.get_sl_schedule_name()

                start = pipeline.sl_start()

                pre_tasks = pipeline.sl_pre_tasks(**kwargs)

                if pre_tasks:
                    pipeline.sl_add_dependency(start, pre_tasks)

                def generate_load_domain(domain: StarlakeDomain):

                    from ai.starlake.common import sanitize_id

                    if schedule_name:
                        name = f"{domain.name}_{schedule_name}"
                    else:
                        name = domain.name

                    with pipeline.sl_create_task_group(group_id=sanitize_id(name)) as load_domain:
                        pld = pipeline.sl_pre_load(
                            domain=domain.name, 
                            tables=set([table.name for table in domain.tables]), 
                            params={'cron':schedule.cron},
                        )
                        with pipeline.sl_create_task_group(group_id=sanitize_id(f'load_{name}')) as load_domain_tables:
                            for table in domain.tables:
                                load_domain_tables.sl_add_task(
                                    pipeline.sl_load(
                                        task_id=sanitize_id(f'load_{domain.name}_{table.name}'), 
                                        domain=domain.name, 
                                        table=table.name,
                                        spark_config=pipeline.get_sl_spark_config(f'{domain.name}.{table.name}'.lower()),
                                        params={'cron':schedule.cron},
                                    )
                                )

                        if pld:
                            load_domain.sl_add_task(pld)
                            pipeline.sl_add_dependency(pld, load_domain_tables)

                        load_domain.sl_add_task(load_domain_tables)

                    return load_domain

                load_domains = [generate_load_domain(domain) for domain in schedule.domains]

                end = pipeline.sl_end(cron=schedule.cron)

                for ld in load_domains:
                    if pre_tasks:
                        pipeline.sl_add_dependency(pre_tasks, ld)
                    else:
                        pipeline.sl_add_dependency(start, ld)
                    pipeline.sl_add_dependency(ld, end)

                post_tasks = pipeline.sl_post_tasks()
            
                if post_tasks:
                    all_done = pipeline.sl_dummy_op(task_id="all_done")
                    for ld in load_domains:
                        pipeline.sl_add_dependency(ld, all_done)
                    pipeline.sl_add_dependency(all_done, post_tasks)
                    pipeline.sl_add_dependency(post_tasks, end)

            return pipeline

        elif self.sl_dependencies:
            # generate the dependencies pipeline
            dependencies = self.sl_dependencies

            with self as pipeline:

                pipeline_id=pipeline.get_sl_pipeline_id()

                resources = pipeline.get_sl_resources()

                cron = pipeline.get_sl_cron()

                datasets: Set[str] = set(map(lambda resource: sanitize_id(resource.uri).lower(), resources or []))

                cron_datasets: dict = {resource.uri: resource.cron for resource in resources or [] if resource.cron is not None and resource.uri is not None}

                first_level_tasks: Set[str] = dependencies.first_level_tasks

                all_dependencies: Set[str] = dependencies.all_dependencies

                load_dependencies: Optional[bool] = pipeline.is_sl_load_dependencies()

                start = pipeline.sl_start()

                pre_tasks = pipeline.sl_pre_tasks()

                if cron:
                    cron_expr = cron
                elif datasets.__len__() == cron_datasets.__len__() and set(cron_datasets.values()).__len__() > 0:
                    sorted_crons = sort_crons_by_frequency(set(
                        cron_datasets.values()), 
                        period=pipeline.get_sl_context_var(var_name='cron_period_frequency', default_value='week')
                    )
                    cron_expr = sorted_crons[0][0]
                else:
                    cron_expr = None

                transform_options = pipeline.get_sl_transform_options(cron_expr) #FIXME

                # create a task
                def create_task(task_id: str, task_name: str, task_type: StarlakeDependencyType):
                    if (task_type == StarlakeDependencyType.task):
                        return pipeline.sl_transform(
                            task_id=task_id, 
                            transform_name=task_name,
                            transform_options=transform_options,
                            spark_config=pipeline.get_sl_spark_config(task_name.lower()),
                            params={'cron':cron, 'cron_expr':cron_expr},
                        )
                    else:
                        load_domain_and_table = task_name.split(".", 1)
                        domain = load_domain_and_table[0]
                        table = load_domain_and_table[1]
                        return pipeline.sl_load(
                            task_id=task_id, 
                            domain=domain, 
                            table=table,
                            spark_config=pipeline.get_sl_spark_config(task_name.lower()),
                            params={'cron':cron},
                        )

                # build group of tasks recursively
                def generate_task_group_for_task(task: StarlakeDependency):
                    task_name = task.name
                    task_group_id = sanitize_id(task_name)
                    task_type = task.dependency_type
                    if (task_type == StarlakeDependencyType.task):
                        task_id = task_group_id + "_task"
                    else:
                        task_id = task_group_id + "_table"

                    children: List[StarlakeDependency] = []
                    if load_dependencies and len(task.dependencies) > 0: 
                        children = task.dependencies
                    else:
                        for child in task.dependencies:
                            if child.name in first_level_tasks:
                                children.append(child)

                    if children.__len__() > 0:
                        with pipeline.sl_create_task_group(group_id=task_group_id) as task_group:
                            for child in children:
                                task_group.sl_add_task(generate_task_group_for_task(child))
                            upstream_tasks = list(task_group.get_sl_tasks())
                            task = create_task(task_id, task_name, task_type)
                            pipeline.sl_add_dependency(upstream_tasks, task)
                        return task_group
                    else:
                        task = create_task(task_id=task_id, task_name=task_name, task_type=task_type)
                        return task

                all_transform_tasks = [generate_task_group_for_task(task) for task in dependencies if task.name not in all_dependencies]

                if pre_tasks:
                    pipeline.sl_add_dependency(start, pre_tasks)
                    pipeline.sl_add_dependency(pre_tasks, all_transform_tasks)
                else:
                    pipeline.sl_add_dependency(start, all_transform_tasks)

                output_resources: List[StarlakeResource] = [
                    StarlakeResource(uri=pipeline_id, cron=cron)
                ]
                if set(cron_datasets.values()).__len__() > 1: # we have at least 2 distinct cron expressions
                    # we sort the cron datasets by frequency (most frequent first)
                    sorted_crons = sort_crons_by_frequency(set(cron_datasets.values()), period=pipeline.get_sl_context_var(var_name='cron_period_frequency', default_value='week'))
                    # we exclude the most frequent cron dataset
                    least_frequent_crons = set([expr for expr, _ in sorted_crons[1:sorted_crons.__len__()]])
                    for dataset, cron in cron_datasets.items() :
                        # we republish the least frequent scheduled datasets
                        if cron in least_frequent_crons:
                            output_resources.append(StarlakeResource(uri=dataset, cron=cron))

                end = pipeline.sl_end(output_resources=output_resources)

                all_transform_tasks >> end

                post_tasks = pipeline.sl_post_tasks()

                if post_tasks:
                    all_done = pipeline.sl_dummy_op(task_id="all_done")
                    pipeline.sl_add_dependency(all_transform_tasks, all_done)
                    pipeline.sl_add_dependency(all_done, post_tasks)
                    pipeline.sl_add_dependency(post_tasks, end)

            return pipeline

        else:
            return None

class StarlakeTaskGroup(Generic[T]):
    def __init__(self, group_id: str, **kwargs) -> None:
        self.group_id = group_id
        self.sl_tasks = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def sl_add_task(self, task: T, **kwargs) -> T:
        self.sl_tasks.append(task)
        return task

    def get_sl_tasks(self) -> List[T]:
        return self.sl_tasks

class StarlakeOrchestration(Generic[U, T, E, J]):
    def __init__(self, job: J, **kwargs) -> None:
        """Generic Starlake orchestration class.
        Args:
            job (J): The job to use.
        """
        if not isinstance(job, IStarlakeJob):
            raise TypeError(f"Expected an instance of IStarlakeJob, got {type(job).__name__}")

        super().__init__(**kwargs) 
        self.job = job

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def sl_create_schedule_pipeline(self, schedule: StarlakeSchedule, nb_schedules: int = 1, **kwargs) -> StarlakePipeline[U, T, E, J]:
        pass

    def sl_create_dependencies_pipeline(self, dependencies: StarlakeDependencies, **kwargs) -> StarlakePipeline[U, T, E, J]:
        pass

    def sl_generate_schedules_pipeline(self, schedules: StarlakeSchedules, **kwargs) -> Union[U, List[U]]:
        """Generate the Starlake dags that will orchestrate the load of the specified domains.

        Args:
            schedules (StarlakeSchedules): The required schedules
        
        Returns:
            List[U]: The generated dags, one for each schedule.
        """
        def generate_pipeline(schedule: StarlakeSchedule, nb_schedules: int = 1):
            return self.sl_create_schedule_pipeline( 
                schedule, 
                nb_schedules=nb_schedules,
            ).sl_generate_pipeline(orchestration=self)

        return [generate_pipeline(schedule, len(schedules)) for schedule in schedules]

    def sl_generate_dependencies_pipeline(self, dependencies: StarlakeDependencies, **kwargs) -> U:
        """Generate the Starlake dag that will orchestrate the specified tasks.

        Args:
            dependencies (StarlakeDependencies): The required dependencies
        
        Returns:
            U: The generated dag.
        """

        return self.sl_create_dependencies_pipeline(dependencies).sl_generate_pipeline(orchestration=self)

class StarlakeOrchestrationFactory:
    _registry = {}

    @classmethod
    def register_orchestration(cls, orchestrator: str, orchestration_class):
        cls._registry[orchestrator] = orchestration_class

    @classmethod
    def create_orchestration(cls, job: J, **kwargs) -> StarlakeOrchestration[U, T, E, J]:
        orchestrator = job.sl_orchestrator()
        if orchestrator not in cls._registry:
            raise ValueError(f"Unknown orchestrator type: {orchestrator}")
        return cls._registry[orchestrator](job, **kwargs)
