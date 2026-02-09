---
name: secure
description: Apply security policies (RLS/CLS)
---

# Secure Skill

This skill applies Row Level Security (RLS) and Column Level Security (CLS) policies.
It shares options with the `load` command.

## Usage

```bash
starlake secure [options]
```

## Options

- `--domains <value>`: Domains to watch
- `--tables <value>`: Tables to watch
- `--accessToken <value>`: Access token to use for authentication
- `--options <value>`: arguments to be used as substitutions
- `--scheduledDate <value>`: Scheduled date for the job

## Examples

### Secure All

```bash
starlake secure
```
