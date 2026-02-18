---
name: xls2yml
description: Convert Excel domain/schema definitions to Starlake YAML
---

# XLS to YML Skill

Converts Excel spreadsheets describing domains and table schemas into Starlake YAML configuration files. This is useful for teams that prefer to manage data models in Excel before generating the YAML configurations.

## Usage

```bash
starlake xls2yml [options]
```

## Options

- `--files <value>`: Comma-separated list of Excel files to convert (required)
- `--iamPolicyTagsFile <value>`: Path to IAM PolicyTag Excel file for BigQuery CLS
- `--outputDir <value>`: Output directory for generated YAML files (default: `metadata/load`)
- `--policyFile <value>`: Optional file for centralizing ACL & RLS definitions
- `--job`: If true, generate YAML for a job definition (instead of domain/table)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Excel File Format

The Excel file contains sheets that define domains and their tables:

- **Domain sheet**: Domain name, description, metadata
- **Table sheets**: One sheet per table with column definitions (name, type, required, comment, etc.)

Sample Excel files are available in `samples/any-engine/metadata/load/`:
- `books.xlsx`
- `tests_csv_json.xlsx`
- `tests_position.xlsx`

## Examples

### Convert Domain Excel to YAML

```bash
starlake xls2yml --files metadata/load/books.xlsx
```

### Convert Multiple Excel Files

```bash
starlake xls2yml --files metadata/load/orders.xlsx,metadata/load/customers.xlsx
```

### Convert with Custom Output Directory

```bash
starlake xls2yml --files metadata/load/books.xlsx --outputDir metadata/load
```

### Convert with IAM Policy Tags

```bash
starlake xls2yml --files metadata/load/books.xlsx --iamPolicyTagsFile metadata/iam-policy-tags.xlsx
```

### Convert with Centralized ACL/RLS Policies

```bash
starlake xls2yml --files metadata/load/books.xlsx --policyFile metadata/policies.yml
```

### Convert Job Definition

```bash
starlake xls2yml --files metadata/jobs/analytics.xlsx --job
```

## Related Skills

- [yml2xls](../yml2xls/SKILL.md) - Convert YAML back to Excel
- [xls2ymljob](../xls2ymljob/SKILL.md) - Convert job Excel to YAML
- [infer-schema](../infer-schema/SKILL.md) - Infer schema from data files
- [config](../config/SKILL.md) - Configuration reference