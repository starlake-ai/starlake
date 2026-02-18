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
- `--compile`: Return final query only (do not execute)
- `--sync-apply`: Update YAML attributes to match SQL query
- `--sync-preview`: Preview YAML attributes to match SQL query
- `--query <value>`: Run this query instead of the task's query
- `--dry-run`: Dry run only
- `--tags <value>`: Run tasks with these tags
- `--format`: Pretty print final query
- `--interactive <value>`: Run query without sinking to the target table. Format: csv, json, table, etc.
- `--reload`: Reload YAML files from disk
- `--truncate`: Force truncation of the target table
- `--recursive`: Execute dependencies recursively
- `--test`: Run as test
- `--accessToken <value>`: Access token to use for authentication (GCP)
- `--options <value>`: Job arguments (key=value,key2=value2)
- `--scheduledDate <value>`: Scheduled date for the job

## Examples

### Run Task

Run the `starbake.customers` task.

```bash
starlake transform --name starbake.customers
```

### Compile Only

Compile the `starbake.customers` task and print the generated SQL. This is useful for debugging.

```bash
starlake transform --name starbake.customers --compile
```

### Dry Run with JSON Output

Run the `starbake.customers` task in dry-run mode and output the result as JSON to the console.

```bash
starlake transform --name starbake.customers --dry-run --interactive json
```

### Run with Recursive Dependencies

Run the `starbake.orders` task and all its dependencies.

```bash
starlake transform --name starbake.orders --recursive
```
