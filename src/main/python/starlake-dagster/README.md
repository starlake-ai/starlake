# starlake-dagster

**starlake-dagster** is the **[Starlake](https://starlake-ai.github.io/starlake/index.html)** Python Distribution for **Dagster**.

It is recommended to use it in combinaison with **[starlake dag generation](https://starlake-ai.github.io/starlake/docs/concepts/orchestration)**, but can be used directly as is in your **DAGs**.

## Installation

```bash
pip install starlake-orchestration[dagster] --upgrade
```

## StarlakeDagsterJob

`ai.starlake.dagster.StarlakeDagsterJob` is an **abstract factory class** that extends the generic factory interface `ai.starlake.job.IStarlakeJob` and is responsible for **generating** the **Dagster node** that will run the `import`, [load](https://starlake-ai.github.io/starlake/docs/concepts/load) and [transform](https://starlake-ai.github.io/starlake/docs/concepts/transform) starlake commands.

### sl_import

It generates the Dagster node that will run the starlake [import](https://starlake-ai.github.io/starlake/docs/cli/import) command.

```python
def sl_import(
    self, 
    task_id: str, 
    domain: str, 
    **kwargs) -> op:
    #...
```

| name    | type | description                                         |
| ------- | ---- | --------------------------------------------------- |
| task_id | str  | the optional task id (`{domain}_import` by default) |
| domain  | str  | the required domain to import                       |

### sl_load

It generates the Dagster node that will run the starlake [load](https://starlake-ai.github.io/starlake/docs/cli/load) command.

```python
def sl_load(
    self, 
    task_id: str, 
    domain: str, 
    table: str, 
    spark_config: StarlakeSparkConfig=None,
    **kwargs) -> op:
    #...
```

| name         | type                | description                                               |
| ------------ | ------------------- | --------------------------------------------------------- |
| task_id      | str                 | the optional task id (`{domain}_{table}_load` by default) |
| domain       | str                 | the required domain of the table to load                  |
| table        | str                 | the required table to load                                |
| spark_config | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`        |

### sl_transform

It generates the Dagster node that will run the starlake [transform](https://starlake-ai.github.io/starlake/docs/cli/transform) command.

```python
def sl_transform(
    self, 
    task_id: str, 
    transform_name: str, 
    transform_options: str=None, 
    spark_config: StarlakeSparkConfig=None, **kwargs) -> op:
    #...
```

| name              | type                | description                                          |
| ----------------- | ------------------- | ---------------------------------------------------- |
| task_id           | str                 | the optional task id (`{transform_name}` by default) |
| transform_name    | str                 | the transform to run                                 |
| transform_options | str                 | the optional transform options                       |
| spark_config      | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`   |

### sl_job

Ultimitely, all these methods will call the `sl_job` method that neeeds to be **implemented** in all **concrete** factory classes.

```python
def sl_job(
    self, 
    task_id: str, 
    arguments: list, 
    spark_config: StarlakeSparkConfig=None, 
    **kwargs) -> op:
    #...
```

| name         | type                | description                                           |
| ------------ | ------------------- | ----------------------------------------------------- |
| task_id      | str                 | the required task id                                  |
| arguments    | list                | The required arguments of the starlake command to run |
| spark_config | StarlakeSparkConfig | the optional `ai.starlake.job.StarlakeSparkConfig`    |

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

`ai.starlake.job.StarlakePreLoadStrategy` is an enum that defines the different **pre load strategies** that can be used to conditionaly load a domain.

The pre load strategy is implemented by `sl_pre_load` method that will generate the Dagster node corresponding to the strategy choosen.

```python
def sl_pre_load(
    self, 
    domain: str, 
    pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None,
    **kwargs) -> op:
    #...
```

| name              | type | description                                                        |
| ----------------- | ---- | ------------------------------------------------------------------ |
| domain            | str  | the domain to load                                                 |
| pre_load_strategy | str  | the optional pre load strategy (self.pre_load_strategy by default) |

##### StarlakePreLoadStrategy.NONE

No pre load strategy.

##### StarlakePreLoadStrategy.IMPORTED

This strategy implies that at least one file is present in the landing area (`SL_ROOT/importing/{domain}` by default if option `incoming_path` has not been specified). If there is one or more files to load, the method `sl_import` will be called to import the domain before loading it, otherwise the loading of the domain will be skipped.

![imported strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagster/imported.png)

##### StarlakePreLoadStrategy.PENDING

This strategy implies that at least one file is present in the pending datasets area of the domain (`SL_ROOT/datasets/pending/{domain}` by default if option `pending_path` has not been specified), otherwise the loading of the domain will be skipped.

![pending strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagster/pending.png)

##### StarlakePreLoadStrategy.ACK

This strategy implies that a **ack file** is present at the specified path (option `global_ack_file_path`), otherwise the loading of the domain will be skipped.

![ack strategy example](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagster/ack.png)

#### Options

The following options can be specified for all concrete factory classes:

| name                     | type | description                                                                               |
| ------------------------ | ---- | ----------------------------------------------------------------------------------------- |
| **sl_env_var**           | str  | optional starlake environment variables passed as an encoded json string                  |
| **pre_load_strategy**    | str  | one of `none` (default), `imported`, `pending` or `ack`                                   |
| **incoming_path**        | str  | path to the landing area for the domain to load (`{SL_ROOT}/incoming` by default)         |
| **pending_path**         | str  | path to the pending datastets for the domain to load (`{SL_DATASETS}/pending` by default) |
| **global_ack_file_path** | str  | path to the ack file (`{SL_DATASETS}/pending/{domain}/{{{{ds}}}}.ack` by default)         |
| **ack_wait_timeout**     | int  | timeout in seconds to wait for the ack file(`1 hour` by default)                          |

## On premise

### StarlakeDagsterShellJob

This class is a concrete implementation of `StarlakeDagsterJob` that generates nodes using dagster-shell library. Usefull for **on premise** execution.

An additional `SL_STARLAKE_PATH` option is required to specify the **path** to the `starlake` **executable**.

#### StarlakeDagsterShellJob Example

The following example shows how to use `StarlakeDagsterShellJob` to generate dynamically Jobs that **load** domains using `starlake` and record corresponding Dagster `assets`.

```python
description="""example to load domain(s) using dagster starlake shell job"""

options = {
    # General options
    'sl_env_var':'{"SL_ROOT": "/starlake/samples/starbake"}', 
    'pre_load_strategy':'imported', 
    # Bash options
    'SL_STARLAKE_PATH':'/starlake/starlake.sh', 
}

from ai.starlake.dagster.shell import StarlakeDagsterShellJob

sl_job = StarlakeDagsterShellJob(options=options)

schedules= [
    {
        'schedule': 'None',
        'cron': '0 0 * * *',
        'domains': [
            {
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
            }
        ]
    }
]

import os

from dagster import ScheduleDefinition, GraphDefinition, Definitions, DependencyDefinition, JobDefinition, In, InputMapping

def load_domain(domain: dict) -> GraphDefinition:
    pld = sl_job.sl_pre_load(domain=domain["name"])
    tables = [table["name"] for table in domain["tables"]]
    ins={"domain": In(str)} if pld else {}
    op_tables = [sl_job.sl_load(task_id=None, domain=domain["name"], table=table, ins=ins) for table in tables]
    input_mappings=[
        InputMapping(
            graph_input_name="domain", mapped_node_name=f"{op_table._name}",
            mapped_node_input_name="domain"
        )
        for op_table in op_tables
    ] if ins else []
    ld = GraphDefinition(
        name=f"{domain['name']}_load",
        node_defs=op_tables,
        dependencies={},
        input_mappings=input_mappings,
    )
    dependencies = dict()
    if pld:
        dependencies[ld._name] = {
            'domain': DependencyDefinition(pld._name, 'asset')
        }
    return GraphDefinition(
        name=f"{domain['name']}",
        node_defs=[ld] + ([pld] if pld else []),
        dependencies=dependencies,
    )

def load_domains(schedule: dict) -> GraphDefinition:
    return GraphDefinition(
        name='schedule',
        node_defs=[load_domain(domain) for domain in schedule["domains"]],        
        dependencies={},
    )

def job_name(schedule) -> str:
    job_name = os.path.basename(__file__).replace(".py", "").replace(".pyc", "").lower()
    return (f"{job_name}-{schedule['schedule']}" if len(schedules) > 1 else job_name)

def generate_job(schedule: dict) -> JobDefinition:
    return JobDefinition(
        name=job_name(schedule),
        description=description,
        graph_def=load_domains(schedule),
    )

crons = []
for schedule in schedules:
    cron = schedule['cron']
    if(cron):
        crons.append(ScheduleDefinition(job_name = job_name(schedule), cron_schedule = cron))

defs = Definitions(
   jobs=[generate_job(schedule) for schedule in schedules],
   schedules=crons,
)

```

![jobs generated with StarlakeDagsterShellJob](https://raw.githubusercontent.com/starlake-ai/starlake/master/src/main/python/images/dagster/jobsWithStarlakeDagsterShellJob.png)

## Cloud
