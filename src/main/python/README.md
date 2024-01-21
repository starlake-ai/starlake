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

Ultimitely, all of these methods will call the `sl_job` method that neeeds to be implemented in concrete classes.

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

### StarlakePreLoadStrategy

`ai.starlake.job.airflow.StarlakePreLoadStrategy` is an enum that defines the different **pre load strategies** that can be used to conditionaly load a domain.

#### NONE

No pre load strategy.

#### IMPORTED

This strategy implies that at least one file is present in the landing area (`SL_ROOT/importing/{domain}` by default if option `incoming_path` has not been specified). If there is one or more files to load, the method `sl_import` will be called to import the domain before loading it, otherwise the loading of the domain will be skipped.

![imported strategy example](images/imported.png)

#### PENDING

This strategy implies that at least one file is present in the pending datasets area of the domain (`SL_ROOT/datasets/pending/{domain}` by default if option `pending_path` has not been specified), otherwise the loading of the domain will be skipped.

![pending strategy example](images/pending.png)

#### ACK

This strategy implies that a **ack file** is present at the specified path (option `global_ack_file_path`), otherwise the loading of the domain will be skipped.

![ack strategy example](images/ack.png)

## On premise

### AirflowStarlakeBashJob

This class is a concrete implementation of `AirflowStarlakeJob` that generates tasks using `airflow.operators.bash.BashOperator`.

## Google Cloud Platform

### AirflowStarlakeDataprocJob

This class is a concrete implementation of `AirflowStarlakeJob` that generates tasks using `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`.

### AirflowStarlakeCloudRunJob

This class is a concrete implementation of `AirflowStarlakeJob` that generates tasks using `airflow.operators.bash.BashOperator` and, if the execution has been parameterized to be **asynchronous**, `airflow.sensors.bash.BashSensor` to interact with **Cloud Run job**.

## Amazon Web Services

## Azure
