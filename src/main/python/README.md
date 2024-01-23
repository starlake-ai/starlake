# starlake-airflow

starlake-airflow is the Starlake Python Distribution for **Airflow**.

It is recommended to use it in combinaison with **starlake dag generation**, but can be used directly as is in your DAGs.

## AirflowStarlakeJob

`ai.starlake.job.airflow.AirflowStarlakeJob` is an **abstract factory class** that extends the generic factory interface `ai.starlake.job.IStarlakeJob` and is responsible for **generating** the **Airflow tasks** that will run the `import`, `load` and `transform` starlake commands.

```python
def sl_import(self, task_id: str, domain: str, **kwargs) -> BaseOperator:
    """Generate the Airflow task that will run the starlake `import` command.

    Args:
        task_id (str): The task id of the task to generate.
        domain (str): The domain to import.

    Returns:
        BaseOperator: The Airflow task.
    """
    #...

def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None,**kwargs) -> BaseOperator:
    """Overrides IStarlakeJob.sl_load()
    Generate the Airflow task that will run the starlake `load` command.

    Args:
        task_id (str): The task id of the task to generate.
        domain (str): The domain to load.
        table (str): The table to load.
        spark_config (StarlakeSparkConfig): The optional spark configuration to use.
  
    Returns:
        BaseOperator: The Airflow task.
    """
    #...

def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
    """Overrides IStarlakeJob.sl_transform()
    Generate the Airflow task that will run the starlake `transform` command.

    Args:
        task_id (str): The task id of the task to generate.
        transform_name (str): The transform to run.
        transform_options (str): The optional transform options to use.
        spark_config (StarlakeSparkConfig): The optional spark configuration to use.
  
    Returns:
        BaseOperator: The Airflow task.
    """
    #...

```

Ultimitely, all of these methods will call the `sl_job` method that neeeds to be implemented in all concrete factory classes.

```python
def sl_job(self, task_id: str, command: str, options: str=None, spark_config: StarlakeSparkConfig=None, **kwargs) -> BaseOperator:
    """Overrides IStarlakeJob.sl_job()
    Generate the Airflow task that will run the starlake command.

    Args:
        task_id (str): The task id of the task to generate.
        command (str): The starlake command to run.
        options (str): The optional options to use.
        spark_config (StarlakeSparkConfig): The optional spark configuration to use.
  
    Returns:
        BaseOperator: The Airflow task.
    """
    pass
```

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

`ai.starlake.job.airflow.StarlakePreLoadStrategy` is an enum that defines the different **pre load strategies** that can be used to conditionaly load a domain.

##### StarlakePreLoadStrategy.NONE

No pre load strategy.

##### StarlakePreLoadStrategy.IMPORTED

This strategy implies that at least one file is present in the landing area (`SL_ROOT/importing/{domain}` by default if option `incoming_path` has not been specified). If there is one or more files to load, the method `sl_import` will be called to import the domain before loading it, otherwise the loading of the domain will be skipped.

![imported strategy example](images/imported.png)

##### StarlakePreLoadStrategy.PENDING

This strategy implies that at least one file is present in the pending datasets area of the domain (`SL_ROOT/datasets/pending/{domain}` by default if option `pending_path` has not been specified), otherwise the loading of the domain will be skipped.

![pending strategy example](images/pending.png)

##### StarlakePreLoadStrategy.ACK

This strategy implies that a **ack file** is present at the specified path (option `global_ack_file_path`), otherwise the loading of the domain will be skipped.

![ack strategy example](images/ack.png)

#### Options

The following options can be specified for all concrete factory classes:

| name                      | type | comment |
| ------------------------- | ---- | ------- |
| **default_pool**          | str  | pool of slots to use (`default_pool` by default)|
| **pre_load_strategy**     | str  | one of `none` (default), `imported`, `pending` or `ack`|
| **incoming_path**         | str  | path to the landing area for the domain to load (`{SL_ROOT}/incoming` by default)|
| **pending_path**          | str  | path to the pending datastets for the domain to load (`{SL_DATASETS}/pending` by default)|
| **global_ack_file_path**  | str  | path to the ack file (`{SL_DATASETS}/pending/{domain}/{{{{ds}}}}.ack` by default)|
| **ack_wait_timeout**      | int  | timeout in seconds to wait for the ack file(`1 hour` by default)|

## On premise

### AirflowStarlakeBashJob

This class is a concrete implementation of `AirflowStarlakeJob` that generates tasks using `airflow.operators.bash.BashOperator`.

## Google Cloud Platform

### AirflowStarlakeDataprocJob

This class is a concrete implementation of `AirflowStarlakeJob` that generates tasks using `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`.

### AirflowStarlakeCloudRunJob

This class is a concrete implementation of `AirflowStarlakeJob` that generates tasks using `airflow.operators.bash.BashOperator` and, if the execution has been parameterized to be **asynchronous**, `airflow.sensors.bash.BashSensor` to wait for the completion of the **Cloud Run job** execution.

## Amazon Web Services

## Azure
