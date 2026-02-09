---
name: test
description: Run tests
---

# Test Skill

This skill runs integration tests for your Starlake project.

## Usage

```bash
starlake test [options]
```

## Options

- `--load`: Test load tasks only
- `--transform`: Test transform tasks only
- `--domain <value>`: Test this domain only
- `--table <value>`: Test this table/task only
- `--test <value>`: Test this specific test only
- `--site`: Generate website results
- `--outputDir <value>`: Output directory
- `--accessToken <value>`: Access token

## Examples

### Run All Tests

```bash
starlake test
```
