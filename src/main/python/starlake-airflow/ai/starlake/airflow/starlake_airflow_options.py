import os

from typing import Optional, TypeVar

from ai.starlake.common import MissingEnvironmentVariable

from ai.starlake.job import StarlakeOptions

from airflow.models import Variable

V = TypeVar("V")

class StarlakeAirflowOptions(StarlakeOptions):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def get_context_var(cls, var_name: str, default_value: Optional[V] = None, options: dict = None, **kwargs):
        """Overrides StarlakeOptions.get_context_var()
        Get the value of the specified variable from the context.
        The value is searched in the following order:
        - options
        - default_value
        - Airflow Variable
        - Environment variable
        Args:
            var_name (str): The variable name.
            default_value (any, optional): The optional default value. Defaults to None.
            options (dict, optional): The optional options dictionary. Defaults to None.
        Raises:
            MissingEnvironmentVariable: If the variable does not exist.
        Returns:
            any: The variable value.
        """
        if options and options.get(var_name):
            return options.get(var_name)
        elif default_value is not None:
            return default_value
        elif Variable.get(var_name, default_var=None, **kwargs) is not None:
            return Variable.get(var_name)
        elif os.getenv(var_name) is not None:
            return os.getenv(var_name)
        else:
            raise MissingEnvironmentVariable(f"{var_name} does not exist")
