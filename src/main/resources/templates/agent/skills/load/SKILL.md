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

- `--domains <value>`: Domains to watch
- `--tables <value>`: Tables to watch
- `--accessToken <value>`: Access token to use for authentication
- `--options <value>`: Watch arguments to be used as substitutions
- `--test`: Should we run this load as a test?
- `--files <value>`: Load this file only
- `--primaryKeys <value>`: Primary keys to set on the table schema
- `--scheduledDate <value>`: Scheduled date for the job

## Examples

### Load All

```bash
starlake load
```

### Load Specific Domain

```bash
starlake load --domains mydomain
```
