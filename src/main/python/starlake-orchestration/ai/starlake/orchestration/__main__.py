import argparse
import importlib.util
import json
import sys
from pathlib import Path

def load_pipelines(module_path):
    module_name = Path(module_path).stem  
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return getattr(module, "pipelines", None)

def parse_options(options_str):
    try:
        # Tenter de parser une chaîne JSON si fournie
        return json.loads(options_str)
    except json.JSONDecodeError:
        # Sinon, gérer le format clé=valeur (ex : key1=value1,key2=value2)
        options = {}
        for option in options_str.split(","):
            if "=" in option:
                key, value = option.split("=", 1)
                options[key.strip()] = value.strip()
        return options

def main():
    parser = argparse.ArgumentParser(description="Execute a Starlake pipeline.")
    parser.add_argument("action", choices=["run", "dry-run", "deploy", "delete", "backfill"], help="Action to be performed on the pipeline.")
    parser.add_argument("--file", required=True, help="Path to the generated DAG file.")
    parser.add_argument("--options", help="Additional options as JSON or key=value pairs (e.g., '{\"key\": \"value\"}' or 'key1=value1,key2=value2').")

    args = parser.parse_args()

    # Charger dynamiquement le pipeline
    pipeline_file = Path(args.file)
    if not pipeline_file.is_file():
        print(f"Error : the file '{pipeline_file}' does not exist.")
        sys.exit(1)

    pipelines = load_pipelines(pipeline_file)

    if not pipelines:
        print(f"Error : No pipeline found in '{pipeline_file}'.")
        sys.exit(1)

    options = parse_options(args.options) if args.options else {}

    if isinstance(pipelines, list):
        for pipeline in pipelines:
            # Map the action to the corresponding method
            action_method = args.action.replace("-", "_")
            if hasattr(pipeline, action_method):
                getattr(pipeline, action_method)(**options)
            else:
                print(f"Error : Method '{action_method}' not defined on pipeline object.")
                sys.exit(1)
    else:
        print(f"Error : The 'pipelines' object is not a list.")
        sys.exit(1)

if __name__ == "__main__":
    main()