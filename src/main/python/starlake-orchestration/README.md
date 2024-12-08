# Starlake Orchestration

## What is Starlake?

Starlake is a **configuration-only** tool for **Extract**, **Load**, **Transform** (**ELT**) and **orchestration** of declarative data pipelines. It simplifies data workflows with minimal coding requirements. Below is a typical use case for Starlake:

1. **Extract**: Gather data from sources such as Fixed Position files, DSV (Delimiter-Separated Values), JSON, or XML formats.
2. **Define or infer structure**: Use YAML to describe or infer the schema for each data source.
3. **Load**: Configure and execute the loading process to your data warehouse or other sinks.
4. **Transform**: Build aggregates and join datasets using SQL, Jinja, and YAML configurations.
5. **Output**: Observe your data becoming available as structured tables in your data warehouse.

Starlake can be used for **any or all** of these steps, providing flexibility to adapt to your workflow.

* **Extract** : Export selective data from SQL databases into CSV files.
* **Preload**: Evaluate whether the loading process should proceed, based on a configurable preload strategy.
* **Load** : Ingest FIXED-WIDTH, CSV, JSON, or XML files, converting them into strongly-typed records stored as Parquet files, data warehouse tables (e.g., Google BigQuery), or other configured sinks.
* **Transform** : Join loaded datasets and save them as Parquet files, data warehouse tables, or Elasticsearch indices.

## What is Starlake orchestration?

Starlake Orchestration is the **Python-based API** for managing and orchestrating data pipelines in Starlake. It offers a simple yet powerful interface for creating, scheduling, and running pipelines while abstracting the complexity of underlying orchestrators.

### Key Features

#### 1. **Supports Multiple Orchestrators**

Starlake Orchestration integrates with popular orchestration frameworks such as **Apache Airflow** and  **Dagster** , enabling you to choose the platform that best fits your needs.

#### 2. **Write Once, Run Anywhere**

The API provides a simple and intuitive way to define pipelines with minimal boilerplate code. Its extensibility ensures pipelines can be reused across different orchestrators.

In alignment with Starlake's philosophy, you can define your data pipelines once and execute them seamlessly across platforms, without rework.

#### 3. **Ensures Data Freshness**

Starlake Orchestration supports  **flexible scheduling mechanisms** , ensuring your data pipelines deliver up-to-date results:

* **Cron-based scheduling** : Define static schedules, such as "run every day at 2 AM."
* **Event-driven orchestration** : Trigger workloads dynamically based on events, utilizing **dataset-aware DAGs** to track data lineage.

By leveraging data lineage and dependencies, Starlake Orchestration aligns schedules automatically, ensuring the freshness of interconnected datasets.

#### 4. **Simplifies Data Pipeline Management**

With automated schedule alignment and dependency management, Starlake Orchestration eliminates manual adjustments and simplifies pipeline workflows, while maintaining reliability.

## What are the main components of Starlake Orchestration?

### IStarlakeJob

`ai.starlake.job.IStarlakeJob` is the **generic factory interface** for creating orchestration tasks. These tasks invoke the appropriate Starlake CLI commands.

Each Starlake command is represented by a factory method. The `sl_job` abstract method must be implemented in all concrete factory classes to handle the creation of specific orchestration tasks (e.g., Dagster `OpDefinition`, Airflow `BaseOperator`, etc.).

#### Abstract Method: `sl_job`

```python
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

#### Factory Methods for Key Starlake Commands

These methods generate tasks for the core Starlake commands. Each method corresponds to a specific operation in the pipeline.

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

1. **NONE** No condition applied; preload tasks are skipped.![none strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/none.png)
2. **IMPORTED** Load only if files exist in the landing area (SL_ROOT/datasets/importing/{domain}).![imported strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/imported.png)
3. **PENDING** Load only if files exist in the pending datasets area (SL_ROOT/datasets/pending/{domain}).![pending strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/pending.png)
4. **ACK** Load only if an acknowledgment file exists at the configured path (global_ack_file_path).![ack strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/ack.png)

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

### StarlakeDataset

When a task is added to the orchestration pipeline, a corresponding `ai.starlake.dataset.StarlakeDataset` object is created. This object encapsulates metadata about the dataset produced by the task.

These dataset objects are collected into a list of events. Each event will be triggered by the orchestrator **only if the corresponding task is completed successfully** during the pipeline's execution.

### StarlakeOptions

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

### AbstractDependency

`ai.starlake.orchestration.AbstractDependency` is an abstract class defining dependencies between tasks or groups of tasks. It is a core part of creating a Directed Acyclic Graph (DAG), ensuring tasks are executed in the correct order.

* **Operators** : Provides the `>>` (downstream dependency) and `<<` (upstream dependency) operators for chaining dependencies intuitively.

### TaskGroupContext

`ai.starlake.orchestration.TaskGroupContext` is a **context manager** responsible for managing the stack of active task groups or pipelines.

* **Key Features** :
  * Ensures that the current context (pipeline or task group) is always available.
  * Automatically adds tasks to the appropriate group.
  * Tracks dependencies within the task group for execution order.

### AbstractTask

`ai.starlake.orchestration.AbstractTask` is an abstract class that defines a unit task. It wraps the concrete orchestration task (e.g., Airflow BaseOperator, Dagster OpDefinition) for a unified interface.

### AbstractTaskGroup

`ai.starlake.orchestration.AbstractTaskGroup` extends `TaskGroupContext`. It defines a group of related tasks, acting as a wrapper around the orchestration-specific group concepts (e.g., Airflow TaskGroup, Dagster GraphDefinition).

### AbstractPipeline

`ai.starlake.orchestration.AbstractPipeline` extends `AbstractTaskGroup` and defines an entire pipeline. It acts as a wrapper around the orchestrator-specific representation of the pipeline (e.g., an **Airflow DAG** or a **Dagster JobDefinition**).

Its main responsibilities include:

* **Task Management** : Managing tasks in the pipeline and adding orchestrator-specific tasks created through the `sl_job` method of the `IStarlakeJob` instance passed to it.
* **Dependency Management** : Adding tasks and managing their dependencies to ensure correct execution order.

This ensures that the pipeline structure is flexible and can adapt to any orchestrator while maintaining a consistent interface and behavior.

The `AbstractPipeline` is instantiated by the concrete `AbstractOrchestration` class via the `sl_create_pipeline` method.

### AbstractOrchestration

`ai.starlake.orchestration.AbstractOrchestration` is the abstract factory class responsible for creating and managing the entire pipeline, task groups, and tasks. It acts as the connection between the Starlake orchestration API and the orchestrator-specific implementation (e.g., Airflow, Dagster).

Two critical abstract methods need to be implemented in concrete orchestration classes:

* **`sl_create_pipeline`** : This method creates an instance of `AbstractPipeline`, defining the overall pipeline structure.
* **`sl_create_task_group`** : This method creates a task group (e.g., Airflow TaskGroup or Dagster GraphDefinition), organizing related tasks.

```python
class MyOrchestration(Generic[U, T, GT, E]):
    ...

    @abstractmethod
    def sl_create_pipeline(self, schedule: Optional[StarlakeSchedule] = None, dependencies: Optional[StarlakeDependencies] = None, **kwargs) -> AbstractPipeline[U, E]:
        """Create a pipeline."""
        pass

    def sl_create_task(self, task_id: str, task: Optional[T], pipeline: AbstractPipeline[U, E]) -> Optional[AbstractTask[T]]:
        """Create a task."""
        if task is None:
            return None
        return AbstractTask(task_id, task)

    @abstractmethod
    def sl_create_task_group(self, group_id: str, pipeline: AbstractPipeline[U, E], **kwargs) -> AbstractTaskGroup[GT]:
        """Create a task group."""
        pass

```

### OrchestrationFactory

`ai.starlake.orchestration.OrchestrationFactory` is responsible for registering and instantiating concrete orchestration classes.

* **Key Features** :
* Maintains a registry of orchestrators.
* Creates instances of `AbstractOrchestration` based on the orchestrator type specified in `IStarlakeJob`.

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
    'pre_load_strategy':'none', 
    'SL_STARLAKE_PATH':'/bin/starlake', 
    'ack_wait_timeout':'60', 
    'load_dependencies':'true', 
    'SL_TIMEZONE':'Europe/Paris', 
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

### Dagster

**[Starlake Dagster](https://pypi.org/project/starlake-dagster/)** is the **[Starlake](https://starlake.ai)** Python Distribution of Starlake **orchestration** for **[Dagster](https://dagster.io/)**.

#### Starlake Dagster Installation

```bash
pip install starlake-orchestration[dagster] --upgrade
```

#### Starlake Dagster Load example

The following example demonstrates how to create a Starlake Dagster job for loading data into a domain.

The only difference between the previous example is the instantiation of the `StarlakeDagsterJob` class instead of the `StarlakeAirflowBashJob` class.

Don't forget, with Starlake Orchestration, **Write once, run anywhere**. :)

```python
from ai.starlake.dagster import StarlakeDagsterJob

sl_job = StarlakeAirflowBashJob(
    filename=os.path.basename(__file__), 
    module_name=f"{__name__}", 
    options=dict(options, **sys.modules[__name__].__dict__.get('jobs', {}))
)
```
