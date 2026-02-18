---
name: load
description: Load data into the data warehouse
---

# Load Skill

This skill loads data from the pending area to the data warehouse.

## Usage

```bash
starlake load [options]
```

## Options

- `--domains <value>`: Comma separated list of domains to watch (default: all)
- `--tables <value>`: Comma separated list of tables to watch (default: all)
- `--accessToken <value>`: Access token to use for authentication (GCP)
- `--options <value>`: Watch arguments to be used as substitutions
- `--test`: Should we run this load as a test?
- `--files <value>`: Load this file only (fully qualified path)
- `--primaryKeys <value>`: Primary keys to set on the table schema (when inferring schema)
- `--scheduledDate <value>`: Scheduled date for the job

## Examples

### Load All

Load all files from the landing area (defined in `_config.sl.yml` in the domain folder) to the pending area and then to the data warehouse.

```bash
starlake load
```

### Load Specific Domain

Only load files belonging to the `starbake` domain.

```bash
starlake load --domains starbake
```

### Load Specific Table

Only load files belonging to the `customers` table in the `starbake` domain.

```bash
starlake load --domains starbake --tables customers
```

### Test Load

Run the load process in test mode. This will not commit changes to the data warehouse.

```bash
starlake load --test
```
