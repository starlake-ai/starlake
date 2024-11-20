from dagster import Definitions
import importlib.util
import glob
import os

# Define the path and any files to exclude
path = "dags"
excluded_files = ["definitions.py", "__init__.py"]

# Recursively find all `.py` files and exclude specified files
python_files = [
    file for file in glob.glob(f"{path}/**/*.py", recursive=True)
    if os.path.basename(file) not in excluded_files
]

import sys

def load_module_with_defs(file_path) :
    directory_path = os.path.dirname(file_path)
    package_name = directory_path.replace("/", ".")
    module_name = os.path.splitext(os.path.basename(file_path))[0]
    spec = importlib.util.spec_from_file_location(f"{package_name}.{module_name}", file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[f"{package_name}.{module_name}"] = module

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        print(f"Error loading module {module}: {e}")
        return None

    # Ensure `defs` exists in the module
    defs: Definitions = getattr(module, "defs", None)
    if defs is not None:
        return f"{package_name}.{module_name}"
    else:
        return None

modules = list()
for file in python_files:
    module = load_module_with_defs(file)
    if module is not None:
        modules.append(module)

print(modules)

with open("pyproject.toml", "w") as f:
    f.write("[tool.dagster]\n")
    if modules:
        f.write("modules = [" + ",".join([f'{{ type = "module", name = "{module}" }}' for module in modules]) + "]\n")

with open("workspace.yaml", "w") as f:
    f.write("load_from:\n")
    for module in modules:
        f.write(f"  - python_module: {module}\n")
