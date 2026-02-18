---
name: yml2xls
description: Convert Starlake YAML definitions to Excel spreadsheets
---

# YML to XLS Skill

Converts Starlake YAML domain/table definitions back into Excel spreadsheets. This is useful for sharing data models with non-technical stakeholders or for round-tripping between YAML and Excel workflows.

## Usage

```bash
starlake yml2xls [options]
```

## Options

- `--domain <value>`: Comma-separated list of domains to convert (default: all)
- `--iamPolicyTagsFile <value>`: IAM PolicyTag YAML file to include (default: `metadata/iam-policy-tags.sl.yml`)
- `--xls <value>`: Output directory for generated Excel files (required)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Convert All Domains to Excel

```bash
starlake yml2xls --xls /tmp/excel
```

### Convert Specific Domain

```bash
starlake yml2xls --domain starbake --xls /tmp/excel
```

### Convert Multiple Domains

```bash
starlake yml2xls --domain starbake,sales --xls /tmp/excel
```

### Convert with IAM Policy Tags

```bash
starlake yml2xls --domain starbake --xls /tmp/excel --iamPolicyTagsFile metadata/iam-policy-tags.sl.yml
```

## Related Skills

- [xls2yml](../xls2yml/SKILL.md) - Convert Excel back to YAML
- [xls2ymljob](../xls2ymljob/SKILL.md) - Convert job Excel to YAML
- [config](../config/SKILL.md) - Configuration reference