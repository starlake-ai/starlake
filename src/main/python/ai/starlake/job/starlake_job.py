from __future__ import annotations

import json
import os

from ai.starlake.common import MissingEnvironmentVariable

from ai.starlake.job.starlake_pre_load_strategy import StarlakePreLoadStrategy

from typing import Generic, TypeVar

T = TypeVar("T")

class IStarlakeJob(Generic[T]):
    def __init__(self, pre_load_strategy: StarlakePreLoadStrategy|str|None, options: dict, **kwargs) -> None:
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

        self.sl_env_vars = self.get_sl_env_vars()
        self.sl_root = __class__.get_context_var(var_name='SL_ROOT', default_value='file://tmp', options=self.sl_env_vars)
        self.sl_datasets = __class__.get_context_var(var_name='SL_DATASETS', default_value=f'{self.sl_root}/datasets', options=self.sl_env_vars)

    def sl_import(self, task_id: str, domain: str, **kwargs) -> T:
        """Import job."""
        pass

    def sl_pre_load(self, domain: str, pre_load_strategy: StarlakePreLoadStrategy|str|None=None, **kwargs) -> T|None:
        """Pre-load job."""
        pass

    def sl_load(self, task_id: str, domain: str, table: str, **kwargs) -> T:
        """Load job."""
        pass

    def sl_transform(self, task_id: str, transform_name: str, transform_options: str=None, **kwargs) -> T:
        """Transform job."""
        pass

    def pre_tasks(self, *args, **kwargs) -> T|None:
        """Pre tasks."""
        return None

    def post_tasks(self, *args, **kwargs) -> T|None:
        """Post tasks."""
        return None

    def sl_job(self, task_id: str, arguments: dict, **kwargs) -> T:
        """Generic job."""
        pass

    def get_sl_env_vars(self) -> dict:
        """Get SL environment variables"""
        try:
            return json.loads(__class__.get_context_var(var_name="sl_env_var", options=self.options))
        except MissingEnvironmentVariable:
            return {}

    @classmethod
    def get_context_var(cls, var_name: str, default_value: any=None, options: dict = None, **kwargs):
        """Get context variable."""
        if options and options.get(var_name):
            return options.get(var_name)
        elif default_value is not None:
            return default_value
        elif os.getenv(var_name) is not None:
            return os.getenv(var_name)
        else:
            raise MissingEnvironmentVariable(f"{var_name} does not exist")

