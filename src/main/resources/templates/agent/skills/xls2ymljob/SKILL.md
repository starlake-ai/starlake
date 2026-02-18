---
name: xls2ymljob
description: Convert Excel job definitions to Starlake YAML
---

# XLS to YML Job Skill

Converts Excel spreadsheets describing transform job definitions into Starlake YAML task configuration files. This is a specialized variant of `xls2yml` focused on job/task definitions.

## Usage

```bash
starlake xls2ymljob [options]
```

## Options

- `--files <value>`: Comma-separated list of Excel files to convert (required)
- `--policyFile <value>`: Optional file for centralizing ACL & RLS definitions
- `--outputDir <value>`: Output directory for generated YAML files
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Convert Job Excel to YAML

```bash
starlake xls2ymljob --files metadata/jobs/analytics.xlsx
```

### Convert with Custom Output

```bash
starlake xls2ymljob --files metadata/jobs/analytics.xlsx --outputDir metadata/transform
```

### Convert with Access Policies

```bash
starlake xls2ymljob --files metadata/jobs/analytics.xlsx --policyFile metadata/policies.yml
```

## Related Skills

- [xls2yml](../xls2yml/SKILL.md) - Convert domain/table Excel to YAML
- [yml2xls](../yml2xls/SKILL.md) - Convert YAML back to Excel
- [transform](../transform/SKILL.md) - Run transform tasks