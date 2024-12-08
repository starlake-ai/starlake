# Starlake Orchestration

## What is Starlake?

Starlake is a **configuration-driven** platform designed to simplify **Extract**, **Load**, and **Transform** (**ELT**) operations while supporting declarative **orchestration** of data pipelines. By minimizing coding requirements, it empowers users to create robust data workflows with YAML-based configurations.

### Typical Use Case

1. **Extract**: Gather data from sources such as Fixed Position files, DSV (Delimiter-Separated Values), JSON, or XML formats.
2. **Define or infer structure**: Use YAML to describe or infer the schema for each data source.
3. **Load**: Configure and execute the loading process to your data warehouse or other sinks.
4. **Transform**: Build aggregates and join datasets using SQL, Jinja, and YAML configurations.
5. **Output**: Observe your data becoming available as structured tables in your data warehouse.

### Flexibility Across Workflows

Starlake supports **any or all** steps in your data pipeline, allowing for seamless integration into existing workflows:

* **Extract** : Export selective data from SQL databases into CSV files.
* **Preload**: Evaluate whether the loading process should proceed, based on a configurable preload strategy.
* **Load** : Ingest FIXED-WIDTH, CSV, JSON, or XML files, converting them into strongly-typed records stored as Parquet files, data warehouse tables (e.g., Google BigQuery), or other configured sinks.
* **Transform** : Join loaded datasets and save them as Parquet files, data warehouse tables, or Elasticsearch indices.

## What is Starlake orchestration?

Starlake Orchestration is a **Python-based API** for creating, scheduling and managing data pipelines. It abstracts the complexities of various orchestration platforms (e.g.,  **Apache Airflow** ,  **Dagster**), offering a unified interface for pipeline orchestration.

Starlake Orchestration aligns with Starlake's philosophy by abstracting orchestration complexities into a simple, unified Python-based API.

It is recommended to use it in combinaison with **[starlake dag generation](https://starlake.ai/starlake/docs/guides/orchestrate/customization)**, but can be used directly as is in your **DAGs**.

### Key Features

#### 1. **Multi-Orchestrator Support**

Starlake Orchestration integrates seamlessly with frameworks like **Apache Airflow** and  **Dagster**, letting you select the best fit for your requirements.

#### 2. **Write Once, Deploy Anywhere**

Define pipelines with minimal boilerplate code and execute them seamlessly across various orchestrators and execution environments—without any rework.
Whether you are using **Dagster** or **Airflow** on **Google Cloud Platform** ( **GCP** ), **Amazon Web Services** ( **AWS** ), or on-prem, Starlake ensures consistent pipeline definitions. Execute Starlake jobs via  **GCP Cloud Run** ,  **GCP Dataproc** ,  **AWS Fargate** , or even through  **shell scripts** .
This adaptability enables teams to transition between execution environments or integrate with diverse cloud-native and on-prem orchestration tools effortlessly.

#### 3. **Data Freshness and Scheduling**

Starlake Orchestration supports  **flexible scheduling mechanisms**, ensuring your data pipelines deliver up-to-date results:

* **Cron-based Scheduling** : Automate periodic pipeline runs (e.g., "Run at 2 AM daily").
* **Event-Driven Orchestration** : Dynamically trigger pipelines using **dataset-aware DAGs**, ensuring dependencies and lineage are respected.

By leveraging data lineage and dependencies, Starlake Orchestration aligns schedules automatically, ensuring the freshness of interconnected datasets.

#### 4. **Simplified Management**

With automated schedule alignment and dependency management, Starlake Orchestration eliminates manual adjustments and simplifies pipeline workflows, while maintaining reliability.

## What are the main components of Starlake Orchestration?

Starlake Orchestration provides a modular and extensible framework for creating, scheduling, and managing data pipelines. Below are the primary components and their roles within the orchestration system:

### 1. IStarlakeJob

`ai.starlake.job.IStarlakeJob` serves as the **generic factory interface** for creating orchestration tasks. These tasks execute the appropriate Starlake CLI commands, allowing seamless integration with orchestration platforms.

#### Key Methods

##### `sl_orchestrator`

Returns the orchestrator type (e.g., `StarlakeOrchestrator.airflow`) for a concrete implementation. This is critical for the `OrchestrationFactory` to instantiate the correct `AbstractOrchestration`.

```python
@abstractmethod
def sl_orchestrator(self) -> Union[StarlakeOrchestrator, str]:
    #...
```

##### `sl_job`

Creates orchestrator-specific tasks (e.g., Airflow `BaseOperator` or Dagster `OpDefinition`).

```python
@abstractmethod
def sl_job(
    self, 
    task_id: str, 
    arguments: list, 
    spark_config: StarlakeSparkConfig=None, 
    **kwargs) -> T:
    #...
```

| name         | type                | description                                           |
| ------------ | ------------------- | ----------------------------------------------------- |
| task_id      | str                 | the required task id                                  |
| arguments    | list                | The required arguments of the starlake command to run |
| spark_config | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`  |

#### Factory Methods for Core Starlake Commands

Each method corresponds to a specific Starlake command.

##### Preload

Will generate the task that will run the starlake [preload](https://starlake.ai/starlake/docs/cli/preload) command.

```python
def sl_pre_load(
    self, 
    domain: str, 
    tables: set=set(), 
    pre_load_strategy: Union[StarlakePreLoadStrategy, str, None] = None, 
    **kwargs) -> Optional[T]:
    #...
```

| name              | type | description                                                       |
| ----------------- | ---- | ----------------------------------------------------------------- |
| domain            | str  | the domain to preload                                             |
| tables            | set  | the optional tables to preload                                    |
| pre_load_strategy | str  | the optional preload strategy (self.pre_load_strategy by default) |

###### StarlakePreLoadStrategy

`ai.starlake.job.StarlakePreLoadStrategy` is an enumeration defining preload strategies for conditional domain loading.

Strategies:

1. **NONE**
   No condition applied; preload tasks are skipped.![none strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/none.png)
2. **IMPORTED**
   Load only if files exist in the landing area (SL_ROOT/datasets/importing/{domain}).![imported strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/imported.png)
3. **PENDING**
   Load only if files exist in the pending datasets area (SL_ROOT/datasets/pending/{domain}).![pending strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/pending.png)
4. **ACK**
   Load only if an acknowledgment file exists at the configured path (global_ack_file_path).![ack strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/ack.png)

##### Import

Generates the task for the [import](https://starlake.ai/starlake/docs/cli/import) command.

```python
def sl_import(
    self, 
    task_id: str, 
    domain: str, 
    tables: set=set(),
    **kwargs) -> T:
    #...
```

| name    | type | description                                           |
| ------- | ---- | ----------------------------------------------------- |
| task_id | str  | the optional task id (`{domain}_import` by default) |
| domain  | str  | the required domain to import                         |
| tables  | set  | the optional tables to import                         |

##### Load

Generates the task for the [load](https://starlake.ai/starlake/docs/cli/load) command.

```python
def sl_load(
    self, 
    task_id: str, 
    domain: str, 
    table: str, 
    spark_config: StarlakeSparkConfig=None,
    **kwargs) -> BaseOperator:
    #...
```

| name         | type                | description                                                 |
| ------------ | ------------------- | ----------------------------------------------------------- |
| task_id      | str                 | the optional task id (`{domain}_{table}_load` by default) |
| domain       | str                 | the required domain of the table to load                    |
| table        | str                 | the required table to load                                  |
| spark_config | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`        |

##### Transform

Generates the task for the [transform](https://starlake.ai/starlake/docs/cli/transform) command.

```python
def sl_transform(
    self, 
    task_id: str, 
    transform_name: str, 
    transform_options: str=None, 
    spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
    #...
```

| name              | type                | description                                            |
| ----------------- | ------------------- | ------------------------------------------------------ |
| task_id           | str                 | the optional task id (`{transform_name}` by default) |
| transform_name    | str                 | the transform to run                                   |
| transform_options | str                 | the optional transform options                         |
| spark_config      | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`   |

### 2. StarlakeDataset

A `ai.starlake.dataset.StarlakeDataset` represents the metadata of a dataset produced by a task.

Starlake orchestration will collect all datasets produced by each task into a list of events to trigger per task.

At run time, the orchestrator will trigger subsequent events only if their corresponding tasks succeed.

### 3. StarlakeOptions

The `ai.starlake.job.StarlakeOptions` class provides methods to manage and retrieve configuration variables used within the context of a Directed Acyclic Graph (DAG). These variables include options passed to the DAG as a dictionary, environment variables, and other runtime configurations.

The following options are available for all concrete factory classes derived from `IStarlakeJob`:

| name                           | type | description                                                                         |
| ------------------------------ | ---- | ----------------------------------------------------------------------------------- |
| **default_pool**         | str  | pool of slots to use (`default_pool` by default)                                  |
| **sl_env_var**           | str  | optional starlake environment variables passed as an encoded json string            |
| **retries**              | int  | optional number of retries to attempt before failing a task (`1` by default)      |
| **retry_delay**          | int  | optional delay between retries in seconds (`300` by default)                      |
| **pre_load_strategy**    | str  | one of `none` (default), `imported`, `pending` or `ack`                     |
| **global_ack_file_path** | str  | path to the ack file (`{SL_DATASETS}/pending/{domain}/{{{{ds}}}}.ack` by default) |
| **ack_wait_timeout**     | int  | timeout in seconds to wait for the ack file(`1 hour` by default)                  |

These options allow you to customize the behavior of the pipeline and the orchestration tasks it defines, providing flexibility for retries, acknowledgment handling, and preload strategies.

### 4. Abstract Classes

#### AbstractDependency

Defines task dependencies, ensuring execution order in Directed Acyclic Graphs (DAGs). Operators such as `>>` and `<<` allow intuitive chaining.

#### AbstractTask

Wraps concrete orchestration tasks (e.g., Airflow operators) into a unified interface.

#### AbstractTaskGroup

Groups related tasks into cohesive units, such as an Airflow `TaskGroup` or a Dagster `GraphDefinition`.

#### AbstractPipeline

Defines an entire pipeline, combining tasks and task groups. It handles:

* **Task management** : Adding and managing orchestrator-specific tasks.
* **Dependency management** : Ensuring the correct execution order.

#### AbstractOrchestration

The central abstraction for creating pipelines, tasks, and task groups. Orchestrator-specific implementations (e.g., for Airflow or Dagster) extend this class.

##### Critical Methods

* **`sl_create_pipeline`** : Creates a pipeline instance, such as an Airflow `DAG` or a Dagster `JobDefinition`.
* **`sl_create_task_group`** : Defines task groups for organizing related tasks, such as an Airflow `TaskGroup` or a Dagster `GraphDefinition`.

### 5. TaskGroupContext

A context manager responsible for:

* Tracking the current task group or pipeline context.
* Automatically adding tasks to the active group.
* Managing dependencies within the group.

### 6. OrchestrationFactory

Handles the dynamic registration and instantiation of concrete orchestration classes.

#### Features

* Maintains a registry of supported orchestrators.
* Dynamically creates `AbstractOrchestration` instances based on the orchestrator type.

```python
class OrchestrationFactory:
    _registry = {}

    @classmethod
    def register_orchestration(cls, orchestrator: str, orchestration_class):
        cls._registry[orchestrator] = orchestration_class

    @classmethod
    def create_orchestration(cls, job: J, **kwargs) -> AbstractOrchestration[U, T, GT, E]:
        orchestrator = job.sl_orchestrator()
        if orchestrator not in cls._registry:
            raise ValueError(f"Unknown orchestrator type: {orchestrator}")
        return cls._registry[orchestrator](job, **kwargs)
```

## How to extend Starlake Orchestration?

### 1. Define a Starlake Job

Create a concrete factory class that implements the `IStarlakeJob` interface. This class is responsible for defining **orchestrator-specific tasks** for the pipeline by overriding the `sl_job` method.

* **Key Responsibilities** :

  * Implement the `sl_job` method to create tasks for your specific orchestrator.
  * Customize task creation logic to integrate with the APIs and conventions of the orchestrator (e.g., Airflow operators, Dagster ops).
* **Example** :

  ```python
  class MyStarlakeJob(IStarlakeJob):
      def sl_job(self, task_id: str, arguments: list, spark_config: StarlakeSparkConfig=None, **kwargs) -> T:
        # Orchestrator-specific implementation for creating a task
        return MyOrchestratorTask(
            task_id=task_id,
            command_arguments=arguments,
            **kwargs
        )
  ```
* Tasks generated by `sl_job` are orchestrator-specific and designed to work seamlessly with the orchestrator chosen for your implementation.

### 2. Implement the Starlake Orchestration API

Extend the `AbstractOrchestration` class to integrate with the orchestrator's API and define the overarching pipeline structure.

* **Key Responsibilities** :
  * Use the `sl_job` method from your `IStarlakeJob` implementation to inject orchestrator-specific tasks into the pipeline.
  * Override abstract methods `sl_create_pipeline` and `sl_create_task_group` to align with orchestrator-specific workflows.

 **Example** :

```python
class MyOrchestration(AbstractOrchestration):
    def sl_create_pipeline(self, schedule=None, dependencies=None, **kwargs):
        # Define pipeline using orchestrator's API
        pipeline = MyOrchestratorPipeline(self.job, dag=None, schedule=schedule, dependencies=dependencies, orchestration=self, **kwargs)
        return pipeline

    def sl_create_task_group(self, group_id: str, pipeline, **kwargs) -> AbstractTaskGroup[GT]:
        # Create task group using orchestrator-specific API
        return AbstractTaskGroup(group_id, MyOrchestratorTaskGroup(name=group_id), **kwargs)
```

* This ensures the orchestration logic is orchestrator-specific.

### 3. Register the Orchestration Class

Register the custom orchestration class in the `OrchestrationFactory`, enabling Starlake to instantiate it based on the orchestrator type.

* Example :

```python
OrchestrationFactory.register_orchestration("my_orchestrator", MyOrchestration)
```

### 4. Create and Run the Pipeline

Leverage the Starlake Orchestration API to define your pipeline, add tasks, and execute the workflow. The `sl_job` implementation ensures that all tasks are compatible with the chosen orchestrator (see examples bellow).

## What are the available Starlake Orchestration distributions?

### Airflow

**[Starlake Airflow](https://pypi.org/project/starlake-airflow/)** is the **[Starlake](https://starlake.ai)** Python Distribution of Starlake **orchestration** for **[Airflow](https://airflow.apache.org/)**.

#### Starlake Airflow Installation

```bash
pip install starlake-orchestration[airflow] --upgrade
```

#### Starlake Airflow Load example

The following example demonstrates how to create a Starlake Airflow pipeline for loading data into a domain.

```python
description="""data loading for starbake domain"""

options={
    'sl_env_var':'{"SL_ROOT": "/demo-starbake", "SL_ENV": "BQ"}', 
    'SL_STARLAKE_PATH':'/bin/starlake', 
    'SL_TIMEZONE':'Europe/Paris',
    'tags':'starbake', 
    'pre_load_strategy':'imported', 
    'ack_wait_timeout':'60', 
    'global_ack_file_path':'/demo-starbake/datasets/pending/starbake/GO.ack'
  
}

import os

import sys

from ai.starlake.airflow.bash import StarlakeAirflowBashJob

sl_job = StarlakeAirflowBashJob(
    filename=os.path.basename(__file__), 
    module_name=f"{__name__}", 
    options=dict(options, **sys.modules[__name__].__dict__.get('jobs', {}))
)

from ai.starlake.job import StarlakePreLoadStrategy

from ai.starlake.dataset import StarlakeDataset

from ai.starlake.orchestration import StarlakeSchedule, StarlakeDomain, StarlakeTable, OrchestrationFactory

from typing import List

schedules= [
    StarlakeSchedule(
        name='daily', 
        cron='0 0 * * *', 
        domains=[
            StarlakeDomain(
                name='starbake', 
                final_name='starbake', 
                tables=[
                    StarlakeTable(
                        name='Customers', 
                        final_name='Customers'
                    ),
                    StarlakeTable(
                        name='Ingredients', 
                        final_name='Ingredients'
                    ),
                    StarlakeTable(
                        name='Products', 
                        final_name='Products'
                    )
                ]
            )
    ]),
    StarlakeSchedule(
        name='hourly', 
        cron='0 * * * *', 
        domains=[
            StarlakeDomain(
                name='starbake', 
                final_name='starbake', 
                tables=[
                    StarlakeTable(
                        name='Orders', 
                        final_name='Orders'
                    )
                ]
            )
    ])
]

with OrchestrationFactory.create_orchestration(job=sl_job) as orchestration:

    def generate_pipeline(schedule: StarlakeSchedule):
        # generate the load pipeline
        with orchestration.sl_create_pipeline(
            schedule=schedule,
        ) as pipeline:

            pipeline_id    = pipeline.pipeline_id
            schedule       = pipeline.schedule
            schedule_name  = pipeline.schedule_name

            start = pipeline.start_task(task_id=f'start_{schedule_name}')
            if not start:
                raise Exception("Start task not defined")

            pre_tasks = pipeline.pre_tasks()

            if pre_tasks:
                start >> pre_tasks

            def generate_load_domain(domain: StarlakeDomain):

                from ai.starlake.common import sanitize_id

                if schedule_name:
                    name = f"{domain.name}_{schedule_name}"
                else:
                    name = domain.name

                with orchestration.sl_create_task_group(group_id=sanitize_id(name), pipeline=pipeline) as ld:

                    pre_load_strategy=pipeline.pre_load_strategy

                    def pre_load(pre_load_strategy: StarlakePreLoadStrategy):
                        if pre_load_strategy != StarlakePreLoadStrategy.NONE:
                            with orchestration.sl_create_task_group(group_id=sanitize_id(f'pre_load_{name}'), pipeline=pipeline) as pre_load_tasks:
                                pre_load = pipeline.sl_pre_load(
                                        domain=domain.name, 
                                        tables=set([table.name for table in domain.tables]), 
                                        params={'cron':schedule.cron, 'schedule': schedule_name}, 
                                    )
                                skip_or_start = pipeline.skip_or_start(
                                    task_id=f'skip_or_start_loading_{name}', 
                                    upstream_task=pre_load
                                )
                                if pre_load_strategy == StarlakePreLoadStrategy.IMPORTED:
                                    sl_import = pipeline.sl_import(
                                            task_id=f"import_{name}",
                                            domain=domain.name, 
                                            tables=set([table.name for table in domain.tables]), 
                                        )
                                else:
                                    sl_import = None

                                if skip_or_start:
                                    pre_load >> skip_or_start
                                    if sl_import:
                                        skip_or_start >> sl_import
                                elif sl_import:
                                    pre_load >> sl_import

                            return pre_load_tasks
                        else:
                            return None

                    pld = pre_load(pre_load_strategy)  

                    def load_domain_tables():
                        with orchestration.sl_create_task_group(group_id=sanitize_id(f'load_{name}'), pipeline=pipeline) as load_domain_tables:
                            for table in domain.tables:
                                pipeline.sl_load(
                                    task_id=sanitize_id(f'load_{domain.name}_{table.name}'), 
                                    domain=domain.name, 
                                    table=table.name,
                                    spark_config=pipeline.sl_spark_config(f'{domain.name}.{table.name}'.lower()),
                                    params={'cron':schedule.cron},
                                )

                        return load_domain_tables

                    ld_tables=load_domain_tables()

                    if pld:
                        pld >> ld_tables

                return ld

            load_domains = [generate_load_domain(domain) for domain in schedule.domains]

            end = pipeline.end_task(
                task_id=f'end_{schedule_name}', 
                output_datasets=[StarlakeDataset(uri=pipeline_id, cron=schedule.cron)]
            )

            if pre_tasks:
                start >> pre_tasks >> load_domains
            else:
                start >> load_domains

            end << load_domains

            post_tasks = pipeline.post_tasks()
  
            if post_tasks:
                all_done = pipeline.sl_dummy_op(task_id="all_done")
                all_done << load_domains
                all_done >> post_tasks >> end

        return pipeline

    [generate_pipeline(schedule) for schedule in schedules]
```

![load starbake ingredients, products and customers daily with imported preload strategy](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/airflow_preload_imported_daily.png)

![load starbake orders hourly with imported preload strategy](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/airflow_preload_imported_hourly.png)

### Dagster

**[Starlake Dagster](https://pypi.org/project/starlake-dagster/)** is the **[Starlake](https://starlake.ai)** Python Distribution of Starlake **orchestration** for **[Dagster](https://dagster.io/)**.

#### Starlake Dagster Installation

```bash
pip install starlake-orchestration[dagster] --upgrade
```

#### Starlake Dagster Load example

The following example demonstrates how to create a Starlake Dagster pipeline for loading data into a domain.

```python
from ai.starlake.dagster import StarlakeDagsterJob

sl_job = StarlakeAirflowBashJob(
    filename=os.path.basename(__file__), 
    module_name=f"{__name__}", 
    options=dict(options, **sys.modules[__name__].__dict__.get('jobs', {}))
)
```

![load starbake ingredients, products and customers daily with imported preload strategy](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagster_preload_imported_daily.png)

![load starbake orders hourly with imported preload strategy](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagster_preload_imported_hourly.png)

The only difference with the previous example is the instantiation of the `StarlakeDagsterJob` class instead of the `StarlakeAirflowBashJob` class.

## Conclusion

Don't forget, with Starlake Orchestration, **Write once, Deploy anywhere**. :)
