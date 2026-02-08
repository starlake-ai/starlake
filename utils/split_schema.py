import os
import json


def extract_schema_properties(schema: dict, keys: list, output_folder: str):
    """
    Extracts schema properties for given keys and saves them to individual
      JSON files.

    Args:
        schema (dict): The schema data loaded from a JSON file.
        keys (list): List of keys to extract schema definitions.
        output_folder (str): Folder where the extracted JSON will be saved.

    Returns:
        dict: A dictionary mapping keys to their output file paths.
    """
    definitions = schema.get("definitions", {})
    output_files = {}

    def find_references(obj, refs):
        """Recursively find all $ref references in the schema."""
        if isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, dict) and "$ref" in v:
                    refs.add(v["$ref"].replace("#/definitions/", ""))
                find_references(v, refs)

    def resolve_references(refs):
        """Resolve all references and extract definitions."""
        extracted, queue = {}, list(refs)
        while queue:
            ref = queue.pop(0)
            if ref in extracted or ref not in definitions:
                continue
            extracted[ref] = definitions[ref]
            find_references(definitions[ref], refs)
            queue.extend(refs - extracted.keys())
        return extracted

    os.makedirs(output_folder, exist_ok=True)

    for key in keys:
        if key not in definitions:
            print(f"Warning: Key '{key}' not found in definitions")
            continue

        refs = set()
        target = definitions[key]
        find_references(target, refs)
        find_references(target.get("properties", {}), refs)

        result = {
            "definitions": resolve_references(refs),
            "properties": target.get("properties", {}),
            "required": target.get("required", []),
        }

        output_file = os.path.join(output_folder, f"starlake_{key}.json")
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)

        output_files[key] = output_file

    return output_files




STARLAKE_JSON_FOLDER = os.getenv("SL_ASK_STARLAKE_JSON_FOLDER")
if STARLAKE_JSON_FOLDER is None:
    # set to current directory / resources
    STARLAKE_JSON_FOLDER = os.path.join(os.getcwd(), "resources")

STARLAKE_JSON_FILE = f"{STARLAKE_JSON_FOLDER}/starlake.json"



with open(STARLAKE_JSON_FILE) as f:
    schema_data = json.load(f)

extract_keys = ["TypeV1", "DagGenerationConfigV1", "DomainV1", "AutoJobDescV1",
                "AutoTaskDescV1", "MapString", "TableV1", "RefV1", "AppConfigV1", "JDBCSchemasV1"]

generated_files = extract_schema_properties(schema_data, extract_keys, STARLAKE_JSON_FOLDER)

# print generated files
for key, file_path in generated_files.items():
    print(f"{key}: {file_path}")