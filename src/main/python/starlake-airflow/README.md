# starlake-airflow

**starlake-airflow** is the **[Starlake](https://starlake.ai)** Python Distribution for **Airflow**.

It is recommended to use it in combinaison with **[starlake dag generation](https://docs.starlake.ai/guides/orchestrate/customization)**, but can be used directly as is in your **DAGs**.

## Prerequisites

Before installing starlake-airflow, ensure the following minimum versions are installed on your system:

- starlake: 1.3.1 or higher
- python: 3.8 or higher
- Apache Airflow: 2.4.0 or higher (2.6.0 or higher is recommanded with cloud-run)

## Installation

```bash
pip install starlake-orchestration[airflow] --upgrade
```

## StarlakeAirflowJob

`ai.starlake.airflow.StarlakeAirflowJob` is an **abstract factory class** that extends the generic factory interface `ai.starlake.job.IStarlakeJob` and is responsible for **generating** the **Airflow tasks** that will run the [import](https://docs.starlake.ai/cli/import), [load](https://docs.starlake.ai/category/load) and [transform](https://docs.starlake.ai/category/transform) starlake commands.

### sl_import

It generates the Airflow task that will run the starlake [import](https://docs.starlake.ai/cli/stage) command.

```python
def sl_import(
    self, 
    task_id: str, 
    domain: str, 
    tables: set=set(),
    **kwargs) -> BaseOperator:
    #...
```

| name    | type | description                                           |
| ------- | ---- | ----------------------------------------------------- |
| task_id | str  | the optional task id (`{domain}_import` by default) |
| domain  | str  | the required domain to import                         |
| tables  | set  | the optional tables to import                         |

### sl_load

It generates the Airflow task that will run the starlake [load](https://docs.starlake.ai/cli/load) command.

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

### sl_transform

It generates the Airflow task that will run the starlake [transform](https://docs.starlake.ai/cli/transform) command.

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

### sl_job

Ultimately, all of these methods will call the `sl_job` method that needs to be **implemented** in all **concrete** factory classes.

```python
def sl_job(
    self, 
    task_id: str, 
    arguments: list, 
    spark_config: StarlakeSparkConfig=None, 
    **kwargs) -> BaseOperator:
    #...
```

| name         | type                | description                                           |
| ------------ | ------------------- | ----------------------------------------------------- |
| task_id      | str                 | the required task id                                  |
| arguments    | list                | The required arguments of the starlake command to run |
| spark_config | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`  |

### Init

To initialize this class, you may specify the optional **pre load strategy** and **options** to use.

```python
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], options: dict=None, **kwargs) -> None:
        """Overrides IStarlakeJob.__init__()
        Args:
            pre_load_strategy (Union[StarlakePreLoadStrategy, str, None]): The pre-load strategy to use.
            options (dict): The options to use.
        """
        super().__init__(pre_load_strategy, options, **kwargs)
        #...
```

#### StarlakePreLoadStrategy

`ai.starlake.job.StarlakePreLoadStrategy` is an enum that defines the different **pre load strategies** that can be used to conditionaly load tables within a domain.

The pre-load strategy is implemented by `sl_pre_load` method that will generate the Airflow group of tasks corresponding to the choosen strategy.

```python
def sl_pre_load(
    self, 
    domain: str, 
    tables: set=set(),
    pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None,
    **kwargs) -> BaseOperator:
    #...
```

| name              | type | description                                                        |
| ----------------- | ---- | ------------------------------------------------------------------ |
| domain            | str  | the domain to load                                                 |
| tables            | set  | the optional tables to pre-load                                                 |
| pre_load_strategy | str  | the optional pre load strategy (self.pre_load_strategy by default) |

##### NONE

The load of the domain will not be conditionned and no pre-load tasks will be executed.

![none strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/none.png)

##### IMPORTED

This strategy implies that at least one file is present in the landing area (`SL_ROOT/datasets/importing/{domain}` by default). If there is one or more files to load, the method `sl_import` will be called to import the domain before loading it, otherwise the loading of the domain will be skipped.

![imported strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/imported.png)

##### PENDING

This strategy implies that at least one file is present in the pending datasets area of the domain (`SL_ROOT/datasets/pending/{domain}` by default), otherwise the loading of the domain will be skipped.

![pending strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/pending.png)

##### ACK

This strategy implies that an **ack file** is present at the specified path (option `global_ack_file_path`), otherwise the loading of the domain will be skipped.

![ack strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/ack.png)

#### Options

The following options can be specified in all concrete factory classes:

| name                           | type | description                                                                                 |
| ------------------------------ | ---- | ------------------------------------------------------------------------------------------- |
| **default_pool**         | str  | pool of slots to use (`default_pool` by default)                                          |
| **tags**         | str  | a list of tags to be applied to the dag                                          |
| **start_date**         | str  | optional start date of the dag                                          |
| **end_date**         | str  | optional end date of the dag                                          |
| **catchup**         | str  | whether to catch up the missed runs or not (`False` by default)                                          |
| **sl_env_var**           | str  | optional starlake environment variables passed as an encoded json string                    |
| **retries**           | int  | optional number of retries to attempt before failing a task (`1` by default)                    |
| **retry_delay**           | int  | optional delay between retries in seconds (`300` by default)                    |
| **pre_load_strategy**    | str  | one of `none` (default), `imported`, `pending` or `ack`                             |
| **global_ack_file_path** | str  | path to the ack file (`{SL_DATASETS}/pending/{domain}/{{{{ds}}}}.ack` by default)         |
| **ack_wait_timeout**     | int  | timeout in seconds to wait for the ack file(`1 hour` by default)                          |

## Data-aware scheduling

The `ai.starlake.airflow.StarlakeAirflowJob` class is also responsible for recording the `outlets` related to the execution of each starlake command, usefull for scheduling DAGs using **data-aware scheduling**.

All the outlets that have been recorded are available in the `outlets` property of the instance of the concrete class.

```python
def __init__(
    self, 
    pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], 
    options: dict=None, 
    **kwargs) -> None:
    #...
    self.outlets: List[Dataset] = kwargs.get('outlets', [])

def sl_import(self, task_id: str, domain: str, tables: set=set(), **kwargs) -> BaseOperator:
    #...
    outlets = self.sl_outlets(domain, **kwargs)
    self.outlets += outlets
    kwargs.update({'outlets': outlets})
    #...

def sl_load(
    self, 
    task_id: str, 
    domain: str, 
    table: str, 
    spark_config: StarlakeSparkConfig=None,
    **kwargs) -> BaseOperator:
    #...
    outlets = self.sl_outlets(domain, **kwargs)
    self.outlets += outlets
    kwargs.update({'outlets': outlets})
    #...

def sl_transform(
    self, 
    task_id: str, 
    transform_name: str, 
    transform_options: str=None, 
    spark_config: StarlakeSparkConfig=None, 
    **kwargs) -> BaseOperator:
    #...
    outlets = self.sl_outlets(domain, **kwargs)
    self.outlets += outlets
    kwargs.update({'outlets': outlets})
    #...
```

In conjonction with the starlake dag generation, the `outlets` property can be used to schedule **effortless** DAGs that will run the **transform** commands.

## On premise

### StarlakeAirflowBashJob

This class is a concrete implementation of `StarlakeAirflowJob` that generates tasks using `airflow.operators.bash.BashOperator`. Usefull for **on premise** execution.

An additional `SL_STARLAKE_PATH` option is required to specify the **path** to the `starlake` **executable**.

#### StarlakeAirflowBashJob load Example

The following example shows how to use `StarlakeAirflowBashJob` to generate dynamically DAGs that **load** domains using `starlake` and record corresponding `outlets`.

```python
description="""example to load domain(s) using airflow starlake bash job"""

options = {
    # General options
    'sl_env_var':'{"SL_ROOT": "/starlake/samples/starbake"}', 
    'pre_load_strategy':'imported', 
    # Bash options
    'SL_STARLAKE_PATH':'/starlake/starlake.sh', 
}

import sys

from ai.starlake.job import StarlakeSparkConfig
from ai.starlake.airflow import StarlakeAirflowOptions

def default_spark_config(*args, **kwargs) -> StarlakeSparkConfig:
    return StarlakeSparkConfig(
        memory=sys.modules[__name__].__dict__.get('spark_executor_memory', None),
        cores=sys.modules[__name__].__dict__.get('spark_executor_cores', None),
        instances=sys.modules[__name__].__dict__.get('spark_executor_instances', None),
        cls_options=StarlakeAirflowOptions(),
        options=options,
        **kwargs
    )
spark_config = getattr(sys.modules[__name__], "get_spark_config", default_spark_config)

from ai.starlake.airflow.bash import StarlakeAirflowBashJob

sl_job = StarlakeAirflowBashJob(options=options)

from ai.starlake.common import sanitize_id

import os

from airflow import DAG

from airflow.datasets import Dataset

from airflow.utils.task_group import TaskGroup

schedules= [{
    'schedule': 'None',
    'cron': None,
    'domains': [{
        'name':'starbake',
        'final_name':'starbake',
        'tables': [
            {
                'name': 'Customers',
                'final_name': 'Customers'
            },
            {
                'name': 'Ingredients',
                'final_name': 'Ingredients'
            },
            {
                'name': 'Orders',
                'final_name': 'Orders'
            },
            {
                'name': 'Products',
                'final_name': 'Products'
            }
        ]
    }]
}]

def generate_dag_name(schedule):
    dag_name = os.path.basename(__file__).replace(".py", "").replace(".pyc", "").lower()
    return (f"{dag_name}-{schedule['schedule']}" if len(schedules) > 1 else dag_name)

# [START instantiate_dag]
for schedule in schedules:
    tags = sl_job.get_context_var(var_name='tags', default_value="", options=options).split()
    for domain in schedule["domains"]:
        tags.append(domain["name"])
    _cron = schedule['cron']
    with DAG(dag_id=generate_dag_name(schedule),
             schedule=_cron,
             default_args=sys.modules[__name__].__dict__.get('default_dag_args', sl_job.default_dag_args()),
             catchup=False,
             tags=list(set([tag.upper() for tag in tags])),
             description=description,
             start_date=sl_job.start_date,
             end_date=sl_job.end_date) as dag:
        start = sl_job.dummy_op(task_id="start")

        post_tasks = sl_job.post_tasks(dag=dag)

        pre_load_tasks = sl_job.sl_pre_load(domain=domain["name"], tables=set([table['name'] for table in domain['tables']]), params={'cron':_cron}, dag=dag)

        def generate_task_group_for_domain(domain):
            with TaskGroup(group_id=sanitize_id(f'{domain["name"]}_load_tasks')) as domain_load_tasks:
                for table in domain["tables"]:
                    load_task_id = sanitize_id(f'{domain["name"]}_{table["name"]}_load')
                    spark_config_name=StarlakeAirflowOptions.get_context_var('spark_config_name', f'{domain["name"]}.{table["name"]}'.lower(), options)
                    sl_job.sl_load(
                        task_id=load_task_id, 
                        domain=domain["name"], 
                        table=table["name"],
                        spark_config=spark_config(spark_config_name, **sys.modules[__name__].__dict__.get('spark_properties', {})),
                        params={'cron':_cron},
                        dag=dag
                    )
            return domain_load_tasks

        all_load_tasks = [generate_task_group_for_domain(domain) for domain in schedule["domains"]]

        if pre_load_tasks:
            start >> pre_load_tasks >> all_load_tasks
        else:
            start >> all_load_tasks

        end = sl_job.dummy_op(task_id="end", outlets=[Dataset(sl_job.sl_dataset(dag.dag_id, cron=_cron), {"source": dag.dag_id})])

        all_load_tasks >> end

        if post_tasks:
            all_done = sl_job.dummy_op(task_id="all_done")
            all_load_tasks >> all_done >> post_tasks >> end
```

![dag generated with StarlakeAirflowBashJob](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagsWithStarlakeAirflowBashJob.png)

#### StarlakeAirflowBashJob Transform Examples

The following example shows how to use `StarlakeAirflowBashJob` to generate dynamically **transform** Jobs using `starlake` and record corresponding `outlets`.

```python
options = {
    # General options
    'sl_env_var':'{"SL_ROOT": "/starlake/samples/starbake"}', 
    'pre_load_strategy':'imported', 
    # Bash options
    'SL_STARLAKE_PATH':'/starlake/starlake.sh', 
}

import sys

from ai.starlake.job import StarlakeSparkConfig
from ai.starlake.airflow import StarlakeAirflowOptions

def default_spark_config(*args, **kwargs) -> StarlakeSparkConfig:
    return StarlakeSparkConfig(
        memory=sys.modules[__name__].__dict__.get('spark_executor_memory', None),
        cores=sys.modules[__name__].__dict__.get('spark_executor_cores', None),
        instances=sys.modules[__name__].__dict__.get('spark_executor_instances', None),
        cls_options=StarlakeAirflowOptions(),
        options=options,
        **kwargs
    )
spark_config = getattr(sys.modules[__name__], "get_spark_config", default_spark_config)

from ai.starlake.airflow.bash import StarlakeAirflowBashJob

#optional variable jobs as a dict of all options to apply by job
#eg jobs = {"task1 domain.task1 name": {"options": "task1 transform options"}, "task2 domain.task2 name": {"options": "task2 transform options"}}
sl_job = StarlakeAirflowBashJob(options=dict(options, **sys.modules[__name__].__dict__.get('jobs', {})))

from ai.starlake.common import sanitize_id, sort_crons_by_frequency, sl_cron_start_end_dates
from ai.starlake.job import StarlakeSparkConfig

import json
import os
import sys
from typing import Set

from airflow import DAG

from airflow.datasets import Dataset

from airflow.utils.task_group import TaskGroup

cron = "None"

_cron = None if cron == "None" else cron

task_deps=json.loads("""[ {
  "data" : {
    "name" : "Customers.HighValueCustomers",
    "typ" : "task",
    "parent" : "Customers.CustomerLifeTimeValue",
    "parentTyp" : "task",
    "parentRef" : "CustomerLifetimeValue",
    "sink" : "Customers.HighValueCustomers"
  },
  "children" : [ {
    "data" : {
      "name" : "Customers.CustomerLifeTimeValue",
      "typ" : "task",
      "parent" : "starbake.Customers",
      "parentTyp" : "table",
      "parentRef" : "starbake.Customers",
      "sink" : "Customers.CustomerLifeTimeValue"
    },
    "children" : [ {
      "data" : {
        "name" : "starbake.Customers",
        "typ" : "table",
        "parentTyp" : "unknown"
      },
      "task" : false
    }, {
      "data" : {
        "name" : "starbake.Orders",
        "typ" : "table",
        "parentTyp" : "unknown"
      },
      "task" : false
    } ],
    "task" : true
  } ],
  "task" : true
} ]""")

load_dependencies: bool = sl_job.get_context_var(var_name='load_dependencies', default_value='False', options=options).lower() == 'true'

datasets: Set[str] = set()

cronDatasets: dict = dict()

_filtered_datasets: Set[str] = set(sys.modules[__name__].__dict__.get('filtered_datasets', []))

from typing import List

first_level_tasks: set = set()

dependencies: set = set()

def load_task_dependencies(task):
    if 'children' in task:
        for subtask in task['children']:
            dependencies.add(subtask['data']['name'])
            load_task_dependencies(subtask)

for task in task_deps:
    task_id = task['data']['name']
    first_level_tasks.add(task_id)
    _filtered_datasets.add(sanitize_id(task_id).lower())
    load_task_dependencies(task)

def _load_datasets(task: dict):
    if 'children' in task:
        for child in task['children']:
            dataset = sanitize_id(child['data']['name']).lower()
            if dataset not in datasets and dataset not in _filtered_datasets:
                childCron = None if child['data'].get('cron') == 'None' else child['data'].get('cron')
                if childCron :
                    cronDataset = sl_job.sl_dataset(dataset, cron=childCron)
                    datasets.add(cronDataset)
                    cronDatasets[cronDataset] = childCron
                else :
                  datasets.add(dataset)

def _load_schedule():
    if _cron:
        schedule = _cron
    elif not load_dependencies : # the DAG will do not depend on any datasets because all the related dependencies will be executed
        for task in task_deps:
            _load_datasets(task)
        schedule = list(map(lambda dataset: Dataset(dataset), datasets))
    else:
        schedule = None
    return schedule

tags = sl_job.get_context_var(var_name='tags', default_value="", options=options).split()

def ts_as_datetime(ts):
  # Convert ts to a datetime object
  from datetime import datetime
  return datetime.fromisoformat(ts)

_user_defined_macros = sys.modules[__name__].__dict__.get('user_defined_macros', dict())
_user_defined_macros["sl_dates"] = sl_cron_start_end_dates
_user_defined_macros["ts_as_datetime"] = ts_as_datetime

catchup: bool = _cron is not None and sl_job.get_context_var(var_name='catchup', default_value='False', options=options).lower() == 'true'

# [START instantiate_dag]
with DAG(dag_id=os.path.basename(__file__).replace(".py", "").replace(".pyc", "").lower(),
         schedule=_load_schedule(),
         default_args=sys.modules[__name__].__dict__.get('default_dag_args', sl_job.default_dag_args()),
         catchup=catchup,
         user_defined_macros=_user_defined_macros,
         user_defined_filters=sys.modules[__name__].__dict__.get('user_defined_filters', None),
         tags=list(set([tag.upper() for tag in tags])),
         description=description,
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
        spark_config_name=StarlakeAirflowOptions.get_context_var('spark_config_name', task_name.lower(), options)
        if (task_type == 'task'):
            return sl_job.sl_transform(
                task_id=airflow_task_id, 
                transform_name=task_name,
                transform_options=transform_options,
                spark_config=spark_config(spark_config_name, **sys.modules[__name__].__dict__.get('spark_properties', {})),
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
                spark_config=spark_config(spark_config_name, **sys.modules[__name__].__dict__.get('spark_properties', {})),
                params={'cron':_cron},
                dag=dag
            )

    # build taskgroups recursively
    def generate_task_group_for_task(task):
        task_name = task['data']['name']
        airflow_task_group_id = sanitize_id(task_name)
        airflow_task_id = airflow_task_group_id
        task_type = task['data']['typ']
        if (task_type == 'task'):
            airflow_task_id = airflow_task_group_id + "_task"
        else:
            airflow_task_id = airflow_task_group_id + "_table"

        children = []
        if load_dependencies and 'children' in task: 
            children = task['children']
        else:
            for child in task.get('children', []):
                if child['data']['name'] in first_level_tasks:
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

    all_transform_tasks = [generate_task_group_for_task(task) for task in task_deps if task['data']['name'] not in dependencies]

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

```

![transform without dependencies](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/bashTransformWithoutDependencies.png)

If you want to load the dependencies, you just need to set the `load_dependencies` option to `True`:

![transform without dependencies](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/bashTransformWithDependencies.png)

## Google Cloud Platform

### StarlakeAirflowDataprocJob

This class is a concrete implementation of `StarlakeAirflowJob` that overrides the `sl_job` method that will run the starlake command by submitting **Dataproc job** to the configured **Dataproc cluster**.

It delegates to an instance of the `ai.starlake.airflow.gcp.StarlakeAirflowDataprocCluster` class the responsibility to :

* **create** the **Dataproc cluster** by instantiating `airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`
* **submit Dataproc job** to the latter by instantiating `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`
* **delete** the **Dataproc cluster** by instantiating `airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator`

This instance is available in the `cluster` property of the `StarlakeAirflowDataprocJob` class and can be configured using the `ai.starlake.airflow.gcp.StarlakeAirflowDataprocClusterConfig` class.

The creation of the **Dataproc cluster** can be performed by calling the `create_cluster` method of the `cluster` property or by calling the `pre_tasks` method of the StarlakeAirflowDataprocJob (the call to the `pre_load` method will, behind the scene, call the `pre_tasks` method and add the optional resulting task to the group of Airflow tasks).

The deletion of the **Dataproc cluster** can be performed by calling the `delete_cluster` method of the `cluster` property or by calling the `post_tasks` method of the StarlakeAirflowDataprocJob.

#### Dataproc cluster configuration

Additional options may be specified to configure the **Dataproc cluster**.

| name                                   | type | description                                                                                                                       |
| -------------------------------------- | ---- | --------------------------------------------------------------------------------------------------------------------------------- |
| **cluster_id**                   | str  | the optional unique id of the cluster that will participate in the definition of the Dataproc cluster name (if not specified)     |
| **dataproc_name**                | str  | the optional dataproc name of the cluster that will participate in the definition of the Dataproc cluster name (if not specified) |
| **dataproc_project_id**          | str  | the optional dataproc project id (the project id on which the composer has been instantiated by default)                          |
| **dataproc_region**              | str  | the optional region (`europe-west1` by default)                                                                                 |
| **dataproc_subnet**              | str  | the optional subnet (the `default` subnet if not specified)                                                                     |
| **dataproc_service_account**     | str  | the optional service account (`service-{self.project_id}@dataproc-accounts.iam.gserviceaccount.com` by default)                 |
| **dataproc_image_version**       | str  | the image version of the dataproc cluster (`2.2-debian1` by default)                                                            |
| **dataproc_master_machine_type** | str  | the optional master machine type (`n1-standard-4` by default)                                                                   |
| **dataproc_master_disk_type**    | str  | the optional master disk type (`pd-standard` by default)                                                                        |
| **dataproc_master_disk_size**    | int  | the optional master disk size (`1024` by default)                                                                               |
| **dataproc_worker_machine_type** | str  | the optional worker machine type (`n1-standard-4` by default)                                                                   |
| **dataproc_worker_disk_type**    | str  | the optional worker disk size (`pd-standard` by default)                                                                        |
| **dataproc_worker_disk_size**    | int  | the optional worker disk size (`1024` by default)                                                                               |
| **dataproc_num_workers**         | int  | the optional number of workers (`4` by default)                                                                                 |
| **dataproc_cluster_metadata**         | str  | the metadata to add to the dataproc cluster specified as a map in json format                                                                                 |

All of these options will be used by default if no **StarlakeAirflowDataprocClusterConfig** was defined when instantiating **StarlakeAirflowDataprocCluster** or if the latter was not defined when instantiating **StarlakeAirflowDataprocJob**.

#### Dataproc Job configuration

Additional options may be specified to configure the **Dataproc job**.

| name                               | type | description                                                                    |
| ---------------------------------- | ---- | ------------------------------------------------------------------------------ |
| **spark_jar_list**           | str  | the required list of spark jars to be used (using `,` as separator)          |
| **spark_bucket**             | str  | the required bucket to use for spark and biqquery temporary storage            |
| **spark_job_main_class**     | str  | the optional main class of the spark job (`ai.starlake.job.Main` by default) |
| **spark_executor_memory**    | str  | the optional amount of memory to use per executor process (`11g` by default) |
| **spark_executor_cores**     | int  | the optional number of cores to use on each executor (`4` by default)        |
| **spark_executor_instances** | int  | the optional number of executor instances (`1` by default)                   |

`spark_executor_memory`, `spark_executor_cores` and `spark_executor_instances` options will be used by default if no **StarlakeSparkConfig** was passed to the `sl_load` and `sl_transform` methods.

#### StarlakeAirflowDataprocJob load Example

The following example shows how to use `StarlakeAirflowDataprocJob` to generate dynamically DAGs that **load** domains using `starlake` and record corresponding `outlets`.

```python
description="""example to load domain(s) using airflow starlake dataproc job"""

options = {
    # General options
    'sl_env_var':'{"SL_ROOT": "gcs://starlake/samples/starbake"}', 
    'pre_load_strategy':'pending', 
    # Dataproc cluster configuration
    'dataproc_project_id':'starbake',
    # Dataproc job configuration 
    'spark_bucket':'my-bucket', 
    'spark_jar_list':'gcs://artifacts/starlake.jar', 
}

from ai.starlake.airflow.gcp import StarlakeAirflowDataprocJob

sl_job = StarlakeAirflowDataprocJob(options=options)

# all the code following the instantiation of the starlake job is exactly the same as that defined for StarlakeAirflowBashJob
#...
```

![dag generated with StarlakeAirflowDataprocJob](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagsWithStarlakeAirflowDataprocJob.png)

### StarlakeAirflowCloudRunJob

This class is a concrete implementation of `StarlakeAirflowJob` that overrides the `sl_job` method that will run the starlake command by executing **Cloud Run job**.

#### Cloud Run job configuration

Additional options may be specified to configure the **Cloud Run job**.

| name                                | type | description                                                                                               |
| ----------------------------------- | ---- | --------------------------------------------------------------------------------------------------------- |
| **cloud_run_project_id**      | str  | the optional cloud run project id (the project id on which the composer has been instantiated by default) |
| **cloud_run_job_name**        | str  | the required name of the cloud run job                                                                    |
| **cloud_run_region**          | str  | the optional region (`europe-west1` by default)                                                         |
| **cloud_run_service_account** | str  | the optional cloud run service account                                                                    |
| **cloud_run_async**           | bool | the optional flag to run the cloud run job asynchronously (`True` by default)                           |
| **retry_on_failure**          | bool | the optional flag to retry the cloud run job on failure (`False` by default)                            |
| **retry_delay_in_seconds**    | int  | the optional delay in seconds to wait before retrying the cloud run job (`10` by default)               |
| **use_gcloud**    | bool  | whether to use the gcloud command or the google cloud run python operator (the gcloud command by default)               |

If the execution has been parameterized to be **asynchronous**, an `airflow.sensors.base.BaseSensorOperator` will be instantiated to wait for the completion of the **Cloud Run job** execution.

#### StarlakeAirflowCloudRunJob load Examples

The following examples shows how to use `StarlakeAirflowCloudRunJob` to generate dynamically DAGs that **load** domains using `starlake` and record corresponding `outlets`.

##### Synchronous execution

```python
description="""example to load domain(s) using airflow starlake cloud run job synchronously"""

options = {
    # General options
    'sl_env_var':'{"SL_ROOT": "gs://my-bucket/starbake"}', 
    'pre_load_strategy':'ack', 
    'global_ack_file_path':'gs://my-bucket/starbake/pending/HighValueCustomers/2024-22-01.ack', 
    # Cloud run options
    'cloud_run_job_name':'starlake', 
    'cloud_run_project_id':'starbake',
    'cloud_run_async':'False'
}

from ai.starlake.airflow.gcp import StarlakeAirflowCloudRunJob

sl_job = StarlakeAirflowCloudRunJob(options=options)
# all the code following the instantiation of the starlake job is exactly the same as that defined for StarlakeAirflowBashJob
#...
```

![dag generated with StarlakeAirflowCloudRunJob synchronously](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagsWithStarlakeAirflowCloudRunJobSynchronous.png)

##### Asynchronous execution

```python

description="""example to load domain(s) using airflow starlake cloud run job asynchronously"""

options = {
    # General options
    'sl_env_var':'{"SL_ROOT": "gs://my-bucket/starbake"}', 
    'pre_load_strategy':'pending', 
    # Cloud run options
    'cloud_run_job_name':'starlake', 
    'cloud_run_project_id':'starbake',
    'cloud_run_async':'True'
    'retry_on_failure':'True', 
}

# all the code following the options is exactly the same as that defined above
#...
```

![dag generated with StarlakeAirflowCloudRunJob asynchronously](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagsWithStarlakeAirflowCloudRunJobAsynchronous.png)

## Amazon Web Services

## Azure
