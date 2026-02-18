---
name: settings
description: Print project settings or test a database connection
---

# Settings Skill

Displays the resolved project settings or tests a specific database connection. Useful for debugging configuration issues and verifying that connections are properly configured.

## Usage

```bash
starlake settings [options]
```

## Options

- `--test-connection <value>`: Test this connection by name (must be defined in `application.sl.yml`)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Print All Settings

```bash
starlake settings
```

### Test a Database Connection

```bash
starlake settings --test-connection duckdb
```

### Test a PostgreSQL Connection

```bash
starlake settings --test-connection source_postgres
```

### Test BigQuery Connection

```bash
starlake settings --test-connection bigquery
```

### Test Snowflake Connection

```bash
starlake settings --test-connection snowflake
```

## Related Skills

- [validate](../validate/SKILL.md) - Validate full project configuration
- [config](../config/SKILL.md) - Configuration reference (connection types)