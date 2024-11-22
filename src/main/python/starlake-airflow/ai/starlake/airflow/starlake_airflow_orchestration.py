from ai.starlake.airflow.starlake_airflow_job import StarlakeAirflowJob, StarlakeSparkConfig

from ai.starlake.airflow.starlake_airflow_options import StarlakeAirflowOptions

from ai.starlake.common import sanitize_id

from ai.starlake.job import StarlakePreLoadStrategy

from ai.starlake.orchestration import IStarlakeOrchestration, StarlakeSchedules, StarlakeSchedule, StarlakeDependencies, StarlakeDomain

from airflow import DAG

from airflow.datasets import Dataset

from airflow.utils.task_group import TaskGroup

from typing import Generic, List, TypeVar, Union

U = TypeVar("U")

class StarlakeAirflowOrchestration(Generic[U], IStarlakeOrchestration[DAG], StarlakeAirflowJob):
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], options: dict=None, **kwargs) -> None:
        """Overrides IStarlakeJob.__init__()
        Args:
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The pre-load strategy to use.
            options (dict): The options to use.
        """
        super().__init__(pre_load_strategy=pre_load_strategy, options=options, **kwargs) 

    def sl_generate_scheduled_tables(self, schedules: StarlakeSchedules, **kwargs) -> List[DAG]:
        """Generate the Starlake dags that will orchestrate the load of the specified domains.

        Args:
            schedules (StarlakeSchedules): The required schedules
        
        Returns:
            list[DAG]: The generated dags, one for each schedule.
        """
        sl_job = self
        options: dict = self.options
        spark_config=self.spark_config

        def generate_dag_name(schedule: StarlakeSchedule):
            dag_name = self.caller_filename.replace(".py", "").replace(".pyc", "").lower()
            return (f"{dag_name}_{schedule.name}" if len(schedules) > 1 else dag_name)

        # [START instantiate_dag]
        for schedule in schedules:
            tags = self.job.get_context_var(var_name='tags', default_value="", options=options).split()
            for domain in schedule.domains:
                tags.append(domain.name)
            _cron = schedule.cron

            with DAG(dag_id=generate_dag_name(schedule),
                    schedule=_cron,
                    default_args=self.caller_globals.get('default_dag_args', sl_job.default_dag_args()),
                    catchup=False,
                    tags=list(set([tag.upper() for tag in tags])),
                    description=self.caller_globals.get('description', ""),
                    start_date=sl_job.start_date,
                    end_date=sl_job.end_date) as dag:
                start = sl_job.dummy_op(task_id="start")

                post_tasks = sl_job.post_tasks(dag=dag)

                pre_load_tasks = sl_job.sl_pre_load(domain=domain.name, tables=set([table.name for table in domain.tables]), params={'cron':_cron}, dag=dag)

                def generate_task_group_for_domain(domain: StarlakeDomain):
                    with TaskGroup(group_id=sanitize_id(f'{domain.name}_load_tasks')) as domain_load_tasks:
                        for table in domain["tables"]:
                            load_task_id = sanitize_id(f'{domain.name}_{table.name}_load')
                            spark_config_name=StarlakeAirflowOptions.get_context_var('spark_config_name', f'{domain.name}.{table.name}'.lower(), options)
                            sl_job.sl_load(
                                task_id=load_task_id, 
                                domain=domain.name, 
                                table=table.name,
                                spark_config=spark_config(spark_config_name, **self.caller_globals.get('spark_properties', {})),
                                params={'cron':_cron},
                                dag=dag
                            )
                    return domain_load_tasks

                all_load_tasks = [generate_task_group_for_domain(domain) for domain in schedule.domains]

                if pre_load_tasks:
                    start >> pre_load_tasks >> all_load_tasks
                else:
                    start >> all_load_tasks

                end = sl_job.dummy_op(task_id="end", outlets=[Dataset(sl_job.sl_dataset(dag.dag_id, cron=_cron), {"source": dag.dag_id})])

                all_load_tasks >> end

                if post_tasks:
                    all_done = sl_job.dummy_op(task_id="all_done")
                    all_load_tasks >> all_done >> post_tasks >> end
                
                return dag

    def sl_generate_scheduled_tasks(self, dependencies: StarlakeDependencies, **kwargs) -> DAG:
        """Generate the Starlake dag that will orchestrate the specified tasks.

        Args:
            dependencies (StarlakeDependencies): The required dependencies
        
        Returns:
            DAG: The generated dag.
        """
        pass
