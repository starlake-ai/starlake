---
name: validate
description: Validate the project and connections
---

# Validate Skill

This skill checks the validity of the project configuration and connections.

## Usage

```bash
starlake validate [options]
```

## Options

- `--reload`: Reload all files from disk before starting validation
- `--inventory`: Print the inventory of all objects in the project

## Examples

### Validate Project

Check the validity of the project configuration, including YAML files, connections, and DAGs.

```bash
starlake validate
```

### Validate and Reload

Reload all configuration files from disk and then validate the project.

```bash
starlake validate --reload
```
