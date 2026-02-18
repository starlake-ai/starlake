---
name: site
description: Generate project documentation website
---

# Site Skill

Generates a documentation website for your Starlake project. The site documents all domains, tables, attributes, transforms, and their relationships. Supports Docusaurus MDX and JSON output formats.

## Usage

```bash
starlake site [options]
```

## Options

- `--outputDir <value>`: Output directory for the generated site
- `--template <value>`: Template name or path to a custom template
- `--format <value>`: Output format: `json` or Docusaurus MDX
- `--json`: Output as JSON
- `--clean`: Clean the output directory before generating
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Generate Documentation Site

```bash
starlake site --outputDir /tmp/docs
```

### Generate as JSON

```bash
starlake site --outputDir /tmp/docs --json
```

### Generate with Custom Template

```bash
starlake site --outputDir /tmp/docs --template /path/to/custom-template
```

### Generate with Clean

```bash
starlake site --outputDir /tmp/docs --clean
```

## Related Skills

- [validate](../validate/SKILL.md) - Validate project before generating docs
- [lineage](../lineage/SKILL.md) - Generate lineage visualizations