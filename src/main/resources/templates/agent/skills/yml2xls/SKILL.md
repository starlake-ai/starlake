---
name: yml2xls
description: Convert YML to XLS
---

# YML 2 XLS Skill

This skill converts Starlake YML definitions into Excel files.

## Usage

```bash
starlake yml2xls [options]
```

## Options

- `--domain <value>`: Domains to convert
- `--iamPolicyTagsFile <value>`: IAM PolicyTag file to convert
- `--xls <value>`: Output directory (required)

## Examples

### Convert Domain to XLS

```bash
starlake yml2xls --domain mydomain --xls /path/to/output
```
