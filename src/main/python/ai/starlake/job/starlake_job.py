from __future__ import annotations

import json
import os

from ai.starlake.common import MissingEnvironmentVariable

from ai.starlake.job.starlake_pre_load_strategy import StarlakePreLoadStrategy
from ai.starlake.job.starlake_options import StarlakeOptions
from ai.starlake.job.spark_config import StarlakeSparkConfig

from typing import Generic, TypeVar, Union

T = TypeVar("T")

class IStarlakeJob(Generic[T], StarlakeOptions):
    def __init__(self, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None], options: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.options = {} if not options else options
        pre_load_strategy = __class__.get_context_var(
            var_name="pre_load_strategy",
            default_value=StarlakePreLoadStrategy.NONE,
            options=self.options
        ) if not pre_load_strategy else pre_load_strategy

        if isinstance(pre_load_strategy, str):
            pre_load_strategy = \
                StarlakePreLoadStrategy(pre_load_strategy) if StarlakePreLoadStrategy.is_valid(pre_load_strategy) \
                    else StarlakePreLoadStrategy.NONE

        self.pre_load_strategy: StarlakePreLoadStrategy = pre_load_strategy

        self.sl_env_vars = __class__.get_sl_env_vars(self.options)
        self.sl_root = __class__.get_sl_root(self.options)
        self.sl_datasets = __class__.get_sl_datasets(self.options)

    def sl_import(self, task_id: str, domain: str, **kwargs) -> T:
        """Import job."""
        pass

    def sl_pre_load(self, domain: str, pre_load_strategy: Union[StarlakePreLoadStrategy, str, None]=None, **kwargs) -> Union[T, None]:
        """Pre-load job."""
        pass

    def sl_load(self, task_id: str, domain: str, table: str, spark_config: StarlakeSparkConfig=None, **kwargs) -> T:
        """Load job."""
        pass

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, spark_config: StarlakeSparkConfig=None, **kwargs) -> T:
        """Transform job."""
        pass

    def pre_tasks(self, *args, **kwargs) -> Union[T, None]:
        """Pre tasks."""
        return None

    def post_tasks(self, *args, **kwargs) -> Union[T, None]:
        """Post tasks."""
        return None

    def sl_job(self, task_id: str, arguments: dict, spark_config: StarlakeSparkConfig=None, **kwargs) -> T:
        """Generic job."""
        pass

