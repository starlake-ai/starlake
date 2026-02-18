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

- `--domain <value>`: The domain list for which to generate extract scripts (comma separated)
- `--template <value>`: Script template dir (required). Containing mustache templates.
- `--audit-schema <value>`: Audit DB schema that will contain the audit export table (required)
- `--delta-column <value>`: The default date column used to determine new rows to export (incremental extraction)

## Examples

### Generate Extraction Scripts

Generate extraction scripts for all domains using templates located in `/path/to/templates`.

```bash
starlake extract-script --template /path/to/templates --audit-schema audit
```
