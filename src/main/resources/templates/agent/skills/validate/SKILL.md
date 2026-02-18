---
name: validate
description: Validate project configuration, YAML files, and connections
---

# Validate Skill

Validates your Starlake project configuration including YAML file syntax, schema compliance, connection configurations, and DAG references. This catches configuration errors before runtime.

## Usage

```bash
starlake validate [options]
```

## Options

- `--reload`: Reload all YAML files from disk before validation
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## What Gets Validated

- **YAML syntax**: All `*.sl.yml` files are parsed and validated
- **Schema compliance**: Files are validated against the Starlake JSON Schema
- **Type references**: Attribute types must match definitions in `types/default.sl.yml`
- **Connection references**: All `connectionRef` values must point to defined connections
- **DAG references**: All `dagRef` values must point to existing DAG configurations
- **Foreign key references**: `foreignKey` attributes must reference existing tables
- **Write strategy consistency**: Key/timestamp columns must exist in attributes

### IDE Validation

For real-time validation in VS Code, add to `.vscode/settings.json`:

```json
{
  "yaml.schemas": {
    "https://json.schemastore.org/starlake.json": [
      "metadata/**/*.sl.yml"
    ]
  }
}
```

## Examples

### Validate Entire Project

```bash
starlake validate
```

### Validate with Reload

Force reload all files from disk before validating:

```bash
starlake validate --reload
```

### Validate with JSON Report

```bash
starlake validate --reportFormat json
```

## Related Skills

- [settings](../settings/SKILL.md) - Test database connections
- [test](../test/SKILL.md) - Run integration tests
- [config](../config/SKILL.md) - Configuration reference