from ai.starlake.airflow.starlake_airflow_job import StarlakeAirflowJob

from ai.starlake.airflow.starlake_airflow_options import StarlakeAirflowOptions

from ai.starlake.common import sanitize_id, sort_crons_by_frequency, sl_cron_start_end_dates

from ai.starlake.orchestration import IStarlakeOrchestration, StarlakeSchedules, StarlakeSchedule, StarlakeDependencies, StarlakeDomain, StarlakeDependency, StarlakeDependencyType

from airflow import DAG

from airflow.datasets import Dataset

from airflow.models.baseoperator import BaseOperator

from airflow.utils.task_group import TaskGroup

from typing import Generic, List, Set, TypeVar, Union

U = TypeVar("U", bound=StarlakeAirflowJob)

class StarlakeAirflowOrchestration(Generic[U], IStarlakeOrchestration[DAG, BaseOperator]):
    def __init__(self, filename: str, module_name: str, job: U, **kwargs) -> None:
        """Overrides IStarlakeOrchestration.__init__()
        Args:
            filename (str): The filename of the orchestration.
            module_name (str): The module name of the orchestration.
            job (U): The job to orchestrate.
        """
        super().__init__(filename, module_name, job, **kwargs) 

    def sl_generate_scheduled_tables(self, schedules: StarlakeSchedules, **kwargs) -> List[DAG]:
        """Generate the Starlake dags that will orchestrate the load of the specified domains.

        Args:
            schedules (StarlakeSchedules): The required schedules
        
        Returns:
            list[DAG]: The generated dags, one for each schedule.
        """
        sl_job = self.job
        options: dict = self.options
        spark_config=self.spark_config
        tags = sl_job.get_context_var(var_name='tags', default_value="", options=options).split()

        def generate_dag(schedule: StarlakeSchedule) -> DAG:
            dag_name = self.caller_filename.replace(".py", "").replace(".pyc", "").lower()

            if len(schedules) > 1:
                dag_id = f"{dag_name}_{schedule.name}"
            else:
                dag_id = dag_name

            for domain in schedule.domains:
                tags.append(domain.name)

            _cron = schedule.cron

            with DAG(dag_id=dag_id,
                    schedule=_cron,
                    default_args=self.caller_globals.get('default_dag_args', sl_job.default_dag_args()),
                    catchup=False,
                    tags=list(set([tag.upper() for tag in tags])),
                    description=self.caller_globals.get('description', ""),
                    start_date=sl_job.start_date,
                    end_date=sl_job.end_date) as dag:
                start = sl_job.dummy_op(task_id="start")

                post_tasks = sl_job.post_tasks(dag=dag)

                def generate_task_group_for_domain(domain: StarlakeDomain):
                    pre_load_tasks = sl_job.sl_pre_load(domain=domain.name, tables=set([table.name for table in domain.tables]), params={'cron':_cron}, dag=dag)

                    with TaskGroup(group_id=sanitize_id(f'{domain.name}_load_tasks')) as domain_load_tasks:
                        for table in domain.tables:
                            sl_job.sl_load(
                                task_id=sanitize_id(f'{domain.name}_{table.name}_load'), 
                                domain=domain.name, 
                                table=table.name,
                                spark_config=spark_config(
                                    sl_job.get_context_var('spark_config_name', f'{domain.name}.{table.name}'.lower(), options), 
                                    **self.caller_globals.get('spark_properties', {})
                                ),
                                params={'cron':_cron},
                                dag=dag
                            )

                    if pre_load_tasks:
                        return start >> pre_load_tasks >> domain_load_tasks
                    else :
                        return start >> domain_load_tasks

                all_load_tasks = [generate_task_group_for_domain(domain) for domain in schedule.domains]

                end = sl_job.dummy_op(task_id="end", outlets=[Dataset(sl_job.sl_dataset(dag.dag_id, cron=_cron), {"source": dag.dag_id})])

                all_load_tasks >> end

                if post_tasks:
                    all_done = sl_job.dummy_op(task_id="all_done")
                    all_load_tasks >> all_done >> post_tasks >> end
                
            return dag

        return [generate_dag(schedule) for schedule in schedules]

    def sl_generate_scheduled_tasks(self, dependencies: StarlakeDependencies, **kwargs) -> DAG:
        """Generate the Starlake dag that will orchestrate the specified tasks.

        Args:
            dependencies (StarlakeDependencies): The required dependencies
        
        Returns:
            DAG: The generated dag.
        """
        sl_job = self.job
        options: dict = self.options
        spark_config=self.spark_config
        tags = self.job.get_context_var(var_name='tags', default_value="", options=options).split()

        cron: Union[str, None] = self.caller_globals.get('cron', None)

        _cron = None if cron is None or cron.lower().strip() == "none" else cron

        load_dependencies: bool = sl_job.get_context_var(var_name='load_dependencies', default_value='False', options=options).lower() == 'true'

        datasets: Set[str] = set()

        cronDatasets: dict = dict()

        _filtered_datasets: Set[str] = set(self.caller_globals.get('filtered_datasets', []))

        first_level_tasks: set = set()

        _dependencies: set = set()

        def load_task_dependencies(task: StarlakeDependency):
            if len(task.dependencies) > 0:
                for subtask in task.dependencies:
                    _dependencies.add(subtask.name)
                    load_task_dependencies(subtask)

        for task in dependencies:
            task_id = task.name
            first_level_tasks.add(task_id)
            _filtered_datasets.add(sanitize_id(task_id).lower())
            load_task_dependencies(task)

        def _load_datasets(task: StarlakeDependency):
            if len(task.dependencies) > 0:
                for child in task.dependencies:
                    dataset = sanitize_id(child.name).lower()
                    if dataset not in datasets and dataset not in _filtered_datasets:
                        childCron = None if child.cron is None else child.cron
                        if childCron :
                            cronDataset = sl_job.sl_dataset(dataset, cron=childCron)
                            datasets.add(cronDataset)
                            cronDatasets[cronDataset] = childCron
                        else :
                            datasets.add(dataset)

        def _load_schedule():
            if _cron:
                schedule = _cron
            elif not load_dependencies : 
                for task in dependencies:
                    _load_datasets(task)
                schedule = list(map(lambda dataset: Dataset(dataset), datasets))
            else: # the DAG will do not depend on any datasets because all the related dependencies will be added as tasks
                schedule = None
            return schedule

        def ts_as_datetime(ts):
            # Convert ts to a datetime object
            from datetime import datetime
            return datetime.fromisoformat(ts)

        _user_defined_macros = self.caller_globals.get('user_defined_macros', dict())
        _user_defined_macros["sl_dates"] = sl_cron_start_end_dates
        _user_defined_macros["ts_as_datetime"] = ts_as_datetime

        catchup: bool = _cron is not None and sl_job.get_context_var(var_name='catchup', default_value='False', options=options).lower() == 'true'

        dag_id = self.caller_filename.replace(".py", "").replace(".pyc", "").lower()

        with DAG(dag_id=dag_id,
                schedule=_load_schedule(),
                default_args=self.caller_globals.get('default_dag_args', sl_job.default_dag_args()),
                catchup=catchup,
                user_defined_macros=_user_defined_macros,
                user_defined_filters=self.caller_globals.get('user_defined_filters', None),
                tags=list(set([tag.upper() for tag in tags])),
                description=self.caller_globals.get('description', ""),
                start_date=sl_job.start_date,
                end_date=sl_job.end_date) as dag:

            start = sl_job.dummy_op(task_id="start")

            pre_tasks = sl_job.pre_tasks(dag=dag)

            post_tasks = sl_job.post_tasks(dag=dag)

            if _cron:
                cron_expr = _cron
            elif datasets.__len__() == cronDatasets.__len__() and set(cronDatasets.values()).__len__() > 0:
                sorted_crons = sort_crons_by_frequency(set(cronDatasets.values()), period=sl_job.get_context_var(var_name='cron_period_frequency', default_value='week', options=options))
                cron_expr = sorted_crons[0][0]
            else:
                cron_expr = None

            if cron_expr:
                transform_options = "{{sl_dates(params.cron_expr, ts_as_datetime(data_interval_end | ts))}}"
            else:
                transform_options = None

            def create_task(airflow_task_id: str, task_name: str, task_type: str):
                spark_config_name=sl_job.get_context_var('spark_config_name', task_name.lower(), options)
                if (task_type == 'task'):
                    return sl_job.sl_transform(
                        task_id=airflow_task_id, 
                        transform_name=task_name,
                        transform_options=transform_options,
                        spark_config=spark_config(spark_config_name, **self.caller_globals.get('spark_properties', {})),
                        params={'cron':_cron, 'cron_expr':cron_expr},
                        dag=dag
                    )
                else:
                    load_domain_and_table = task_name.split(".",1)
                    domain = load_domain_and_table[0]
                    table = load_domain_and_table[1]
                    return sl_job.sl_load(
                        task_id=airflow_task_id, 
                        domain=domain, 
                        table=table,
                        spark_config=spark_config(spark_config_name, **self.caller_globals.get('spark_properties', {})),
                        params={'cron':_cron},
                        dag=dag
                    )

            # build taskgroups recursively
            def generate_task_group_for_task(task: StarlakeDependency):
                task_name = task.name
                airflow_task_group_id = sanitize_id(task_name)
                airflow_task_id = airflow_task_group_id
                task_type = task.dependency_type
                if (task_type == StarlakeDependencyType.task):
                    airflow_task_id = airflow_task_group_id + "_task"
                else:
                    airflow_task_id = airflow_task_group_id + "_table"

                children: List[StarlakeDependency] = []
                if load_dependencies and len(task.dependencies) > 0: 
                    children = task.dependencies
                else:
                    for child in task.dependencies:
                        if child.name in first_level_tasks:
                            children.append(child)

                if children.__len__() > 0:
                    with TaskGroup(group_id=airflow_task_group_id) as airflow_task_group:
                        for transform_sub_task in children:
                            generate_task_group_for_task(transform_sub_task)
                        upstream_tasks = list(airflow_task_group.children.values())
                        airflow_task = create_task(airflow_task_id, task_name, task_type)
                        airflow_task.set_upstream(upstream_tasks)
                    return airflow_task_group
                else:
                    airflow_task = create_task(airflow_task_id=airflow_task_id, task_name=task_name, task_type=task_type)
                    return airflow_task

            all_transform_tasks = [generate_task_group_for_task(task) for task in dependencies if task.name not in _dependencies]

            if pre_tasks:
                start >> pre_tasks >> all_transform_tasks
            else:
                start >> all_transform_tasks

            extra: dict = {"source": dag.dag_id}
            outlets: List[Dataset] = [Dataset(sl_job.sl_dataset(dag.dag_id, cron=_cron), extra)]
            if set(cronDatasets.values()).__len__() > 1: # we have at least 2 distinct cron expressions
                # we sort the cron datasets by frequency (most frequent first)
                sorted_crons = sort_crons_by_frequency(set(cronDatasets.values()), period=sl_job.get_context_var(var_name='cron_period_frequency', default_value='week', options=options))
                # we exclude the most frequent cron dataset
                least_frequent_crons = set([expr for expr, _ in sorted_crons[1:sorted_crons.__len__()]])
                for dataset, cron in cronDatasets.items() :
                    # we republish the least frequent scheduled datasets
                    if cron in least_frequent_crons:
                        outlets.append(Dataset(dataset, extra))

            end = sl_job.dummy_op(task_id="end", outlets=outlets)

            all_transform_tasks >> end

            if post_tasks:
                all_done = sl_job.dummy_op(task_id="all_done")
                all_transform_tasks >> all_done >> post_tasks >> end

        return dag