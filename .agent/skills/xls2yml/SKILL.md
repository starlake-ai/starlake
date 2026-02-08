---
name: xls2yml
description: Convert XLS to YML
---

# XLS 2 YML Skill

This skill converts Excel files describing domains and schemas into Starlake YML definitions.

## Usage

```bash
starlake xls2yml [options]
```

## Options

- `--files <value>`: List of Excel files (required)
- `--iamPolicyTagsFile <value>`: IAM PolicyTag file
- `--outputDir <value>`: Output directory
- `--policyFile <value>`: Policy file
- `--job`: Generate YML for a Job?

## Examples

### Convert Domain XLS

```bash
starlake xls2yml --files /path/to/domain.xlsx
```
