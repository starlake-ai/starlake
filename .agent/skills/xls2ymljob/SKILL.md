---
name: xls2ymljob
description: Convert XLS to YML job
---

# XLS 2 YML Job Skill

This skill converts an Excel file describing a job into a Starlake YML job definition.

## Usage

```bash
starlake xls2ymljob [options]
```

## Options

- `--files <value>`: List of Excel files (required)
- `--policyFile <value>`: Access Policy file
- `--outputDir <value>`: Output directory

## Examples

### Convert Job XLS

```bash
starlake xls2ymljob --files /path/to/job.xlsx
```
