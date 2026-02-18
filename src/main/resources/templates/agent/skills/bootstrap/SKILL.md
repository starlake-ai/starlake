---
name: bootstrap
description: Create a new Starlake project from a template
---

# Bootstrap Skill

Creates a new Starlake project with the standard directory structure and starter configuration files. Optionally uses a named template to scaffold a project with sample data and configurations.

## Usage

```bash
starlake bootstrap [options]
```

## Options

- `--template <value>`: Template name to use (default: interactive selection). Examples: `quickstart`, `simple`
- `--no-exit`: Keep the JVM running after project creation (used in testing)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Generated Project Structure

```
my-project/
├── metadata/
│   ├── application.sl.yml       # Global configuration, connections
│   ├── env.sl.yml               # Environment variables
│   ├── types/
│   │   └── default.sl.yml       # Data type definitions
│   ├── load/
│   │   └── {domain}/
│   │       ├── _config.sl.yml   # Domain configuration
│   │       └── {table}.sl.yml   # Table schemas
│   ├── transform/
│   │   └── {domain}/
│   │       ├── _config.sl.yml   # Transform defaults
│   │       └── {task}.sql       # SQL transformations
│   ├── dags/                    # Orchestration DAG configs
│   ├── expectations/            # Data quality macros
│   └── extract/                 # Extraction configurations
├── datasets/                    # Processed data storage
├── incoming/                    # Raw data landing area
└── sample-data/                 # Example datasets
```

### Key Configuration Files

#### `application.sl.yml`

```yaml
version: 1
application:
  connectionRef: "{{connectionRef}}"
  audit:
    sink:
      connectionRef: "{{connectionRef}}"
  connections:
    duckdb:
      type: "jdbc"
      options:
        url: "jdbc:duckdb:{{SL_ROOT}}/datasets/duckdb.db"
        driver: "org.duckdb.DuckDBDriver"
    bigquery:
      type: "bigquery"
      options:
        location: "europe-west1"
        authType: "APPLICATION_DEFAULT"
  dagRef:
    load: "airflow_load_shell"
    transform: "airflow_transform_shell"
```

#### `env.sl.yml`

```yaml
version: 1
env:
  root_path: "{{SL_ROOT}}"
  incoming_path: "{{SL_ROOT}}/datasets/incoming"
  connectionRef: duckdb
```

## Examples

### Bootstrap with Interactive Selection

```bash
starlake bootstrap
```

### Bootstrap with Quickstart Template

```bash
starlake bootstrap --template quickstart
```

### Bootstrap with Simple Template

```bash
starlake bootstrap --template simple
```

## Related Skills

- [config](../config/SKILL.md) - Configuration reference
- [load](../load/SKILL.md) - Load data into the warehouse
- [validate](../validate/SKILL.md) - Validate project configuration