---
name: acl-dependencies
description: Generate ACL (Access Control List) dependencies graph
---

# ACL Dependencies Skill

Generates a visual graph showing the relationships between users/groups and the tables they have access to, based on ACL (Access Control List) definitions in your YAML configurations.

## Usage

```bash
starlake acl-dependencies [options]
```

## Options

- `--output <value>`: Output file path (default: console output)
- `--grantees <value>`: Comma-separated list of users/groups to include (default: all)
- `--reload`: Reload YAML files from disk before computing
- `--svg`: Generate SVG image
- `--png`: Generate PNG image
- `--json`: Generate JSON output
- `--tables <value>`: Comma-separated list of tables to include (default: all)
- `--all`: Include all ACLs in the output
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

ACL dependencies are derived from `acl` and `rls` definitions in table/task YAML files:

```yaml
# In table.sl.yml or task.sl.yml
table:
  acl:
    - role: "roles/bigquery.dataViewer"
      grants:
        - "user:analyst@domain.com"
        - "group:analytics_team@domain.com"
  rls:
    - name: "USA only"
      predicate: "country = 'USA'"
      grants:
        - "group:usa_team@domain.com"
```

## Examples

### Generate Full ACL Graph as SVG

```bash
starlake acl-dependencies --svg --output acl.svg --all
```

### Filter by Specific Users

```bash
starlake acl-dependencies --grantees "user:analyst@domain.com,group:analytics_team" --svg --output acl.svg
```

### Filter by Specific Tables

```bash
starlake acl-dependencies --tables starbake.orders,starbake.customers --svg --output acl.svg
```

### Generate JSON Output

```bash
starlake acl-dependencies --json --output acl.json --all
```

### Generate PNG Image

```bash
starlake acl-dependencies --png --output acl.png --all
```

## Related Skills

- [secure](../secure/SKILL.md) - Apply RLS/CLS security policies
- [iam-policies](../iam-policies/SKILL.md) - Apply IAM policies
- [lineage](../lineage/SKILL.md) - Task dependency lineage
- [table-dependencies](../table-dependencies/SKILL.md) - Table relationship graph