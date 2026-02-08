---
name: autoload
description: Automatically infer schemas and load data from the incoming directory
---

# AutoLoad Skill

This skill watches the incoming directory, infers schemas for new files, and loads them.

## Usage

```bash
starlake autoload [options]
```

## Options

- `--domains <value>`: Domains to watch
- `--tables <value>`: Tables to watch
- `--clean`: Overwrite existing mapping files before starting
- `--accessToken <value>`: Access token to use for authentication
- `--scheduledDate <value>`: Scheduled date for the job
- `--options <value>`: Watch arguments to be used as substitutions (k1=v1,k2=v2...)

## Examples

### Autoload All

```bash
starlake autoload
```
