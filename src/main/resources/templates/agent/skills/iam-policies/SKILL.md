---
name: iam-policies
description: Apply IAM (Identity and Access Management) policies
---

# IAM Policies Skill

Applies IAM (Identity and Access Management) policies defined in your project configuration. This sets up permissions, roles, and access controls on your data warehouse resources (e.g., BigQuery datasets, tables).

## Usage

```bash
starlake iam-policies [options]
```

## Options

- `--accessToken <value>`: Access token for authentication (e.g. GCP)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

IAM policies are configured in the application and table-level YAML files:

### Application-Level Access Policies

```yaml
# metadata/application.sl.yml
version: 1
application:
  accessPolicies:
    apply: true
    location: EU
    taxonomy: RGPD
```

### Table-Level ACL

```yaml
# In table.sl.yml or task.sl.yml
table:
  acl:
    - role: "roles/bigquery.dataViewer"
      grants:
        - "user:user@domain.com"
        - "group:analytics_team@domain.com"
```

### IAM Policy Tags (Excel-based)

IAM policy tags can be managed via Excel files and converted using `xls2yml`:

```bash
starlake xls2yml --files metadata/iam-policy-tags.xlsx --iamPolicyTagsFile true
```

## Examples

### Apply All IAM Policies

```bash
starlake iam-policies
```

### Apply with Access Token

```bash
starlake iam-policies --accessToken $GCP_TOKEN
```

## Related Skills

- [secure](../secure/SKILL.md) - Apply RLS/CLS security policies
- [xls2yml](../xls2yml/SKILL.md) - Convert IAM policy tags from Excel to YAML
- [config](../config/SKILL.md) - Configuration reference (access policies)