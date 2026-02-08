---
name: extract-script
description: Generate extraction scripts from a template
---

# Extract Script Skill

This skill generates scripts to extract data, based on a Mustache template.

## Usage

```bash
starlake extract-script [options]
```

## Options

- `--domain <value>`: The domain list for which to generate extract scripts
- `--template <value>`: Script template dir (required)
- `--audit-schema <value>`: Audit DB that will contain the audit export table (required)
- `--delta-column <value>`: The default date column used to determine new rows to export

## Examples

### Generate Script

```bash
starlake extract-script --template /path/to/templates --audit-schema audit
```
