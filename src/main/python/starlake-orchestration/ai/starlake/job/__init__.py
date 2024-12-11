__all__ = ['spark_config', 'starlake_job', 'starlake_options', 'starlake_pre_load_strategy']

from .spark_config import StarlakeSparkConfig, StarlakeSparkExecutorConfig
from .starlake_job import IStarlakeJob, StarlakeOrchestrator, StarlakeExecutionEnvironment, StarlakeJobFactory
from .starlake_options import StarlakeOptions
from .starlake_pre_load_strategy import StarlakePreLoadStrategy

import os
import importlib
import inspect

def register_jobs_from_package(package_name: str = "ai.starlake") -> None:
    """
    Dynamically load all classes implementing IStarlakeJob from the given root package, including sub-packages,
    and register them in the StarlakeJobRegistry.
    """
    print(f"Registering jobs from package {package_name}")
    package = importlib.import_module(package_name)
    package_path = os.path.dirname(package.__file__)

    for root, dirs, files in os.walk(package_path):
        # Convert the filesystem path back to a Python module path
        relative_path = os.path.relpath(root, package_path)
        if relative_path == ".":
            module_prefix = package_name
        else:
            module_prefix = f"{package_name}.{relative_path.replace(os.path.sep, '.')}"

        for file in files:
            if file.endswith(".py") and file != "__init__.py":
                module_name = os.path.splitext(file)[0]
                full_module_name = f"{module_prefix}.{module_name}"

                try:
                    module = importlib.import_module(full_module_name)
                except ImportError as e:
                    print(f"Failed to import module {full_module_name}: {e}")
                    continue
                except AttributeError as e:
                    print(f"Failed to import module {full_module_name}: {e}")
                    continue

                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, IStarlakeJob) and obj is not IStarlakeJob:
                        StarlakeJobFactory.register_job(obj)

register_jobs_from_package()
