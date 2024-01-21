# starlake-airflow

starlake-airflow is the Starlake Python Distribution for Airflow.

It is recommended to use it in combinaison with starlake dag generation, but can be used directly as is in your DAGs.

## AirflowStarlakeJob

`AirflowStarlakeJob` is an abstract factory class that extends the generic interface IStarlakeJob and is responsible for generating the Airflow tasks that will run the `import`, `load` and `transform` starlake commands.

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
    """Generate the Airflow task that will run the starlake command.

    Args:
        task_id (str): The task id of the task to generate.
        command (str): The starlake command to run.
        options (str): The optional options to use.
        spark_config (StarlakeSparkConfig): The optional spark configuration to use.
    
    Returns:
        BaseOperator: The Airflow task.
    """
    #...
```

## On premise

## Google Cloud Platform

## Amazon Web Services

## Azure
