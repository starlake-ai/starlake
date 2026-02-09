---
name: transform
description: Run a transformation task
---

# Transform Skill

This skill runs a defined transformation task.

## Usage

```bash
starlake transform [options]
```

## Options

- `--name <value>`: Task Name (domain.task) (required unless tags used)
- `--compile`: Return final query only
- `--sync-apply`: Update YAML attributes to match SQL query
- `--sync-preview`: Preview YAML attributes to match SQL query
- `--query <value>`: Run this query instead of the task's query
- `--dry-run`: Dry run only
- `--tags <value>`: Run tasks with these tags
- `--format`: Pretty print final query
- `--interactive <value>`: Run query without sinking (csv, json, table...)
- `--reload`: Reload YAML files
- `--truncate`: Force truncation
- `--recursive`: Execute dependencies recursively
- `--test`: Run as test
- `--accessToken <value>`: Access token
- `--options <value>`: Job arguments
- `--scheduledDate <value>`: Scheduled date

## Examples

### Run Task

```bash
starlake transform --name mydomain.mytask
```

### Dry Run with JSON Output

```bash
starlake transform --name mydomain.mytask --dry-run --interactive json
```
