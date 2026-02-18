---
name: extract-script
description: Generate extraction scripts from Mustache/SSP templates
---

# Extract Script Skill

Generates extraction scripts from Mustache or SSP (Scalate) templates. This is useful for generating custom SQL DDL scripts (CREATE, ALTER, DROP) for different database platforms.

## Usage

```bash
starlake extract-script [options]
```

## Options

- `--domain <value>`: Comma-separated list of domains to generate scripts for (default: all)
- `--template <value>`: Path to the template directory containing Mustache/SSP templates (required)
- `--audit-schema <value>`: Audit DB schema that will contain the audit export table (required)
- `--delta-column <value>`: Default date column used for incremental extraction tracking
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

### DDL Templates

Templates are organized by database platform in `metadata/extract/ddl/`:

```
metadata/extract/ddl/
├── bigquery/
│   ├── create.ssp
│   ├── alter.ssp
│   └── drop.ssp
├── postgres/
│   └── drop.ssp
└── synapse/
    ├── create.ssp
    ├── alter.ssp
    └── drop.ssp
```

### Template Variables

Templates receive the following variables:
- Domain/schema information (name, tables)
- Table attributes (columns, types, constraints)
- Database-specific type mappings from `types/default.sl.yml`

## Examples

### Generate DDL Scripts for All Domains

```bash
starlake extract-script --template metadata/extract/ddl/bigquery --audit-schema audit
```

### Generate Scripts for Specific Domains

```bash
starlake extract-script --domain sales,hr --template metadata/extract/ddl/postgres --audit-schema audit
```

### Generate with Delta Column for Incremental

```bash
starlake extract-script --template metadata/extract/ddl/synapse --audit-schema audit --delta-column updated_at
```

## Related Skills

- [extract-schema](../extract-schema/SKILL.md) - Extract schema from database
- [yml2ddl](../yml2ddl/SKILL.md) - Generate DDL from YAML definitions
- [extract](../extract/SKILL.md) - Extract both schema and data