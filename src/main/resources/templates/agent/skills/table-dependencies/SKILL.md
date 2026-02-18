---
name: table-dependencies
description: Generate table dependency graph based on foreign key relationships
---

# Table Dependencies Skill

Generates a visual graph showing table dependencies based on foreign key relationships defined in your YAML configurations. This is an entity-relationship diagram for your data model.

## Usage

```bash
starlake table-dependencies [options]
```

## Options

- `--output <value>`: Output file path (default: console output)
- `--all-attrs`: Include all attributes in the diagram (not just primary/foreign keys). Default: true
- `--reload`: Reload YAML files from disk before computing
- `--svg`: Generate SVG image
- `--png`: Generate PNG image
- `--json`: Generate JSON output
- `--related`: Include only entities that have relationships to other entities
- `--tables <value>`: Comma-separated list of specific tables to include
- `--all`: Include all tables (default)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Configuration Context

Table dependencies are derived from `foreignKey` attributes in table definitions:

```yaml
# metadata/load/starbake/order_lines.sl.yml
table:
  name: "order_lines"
  primaryKey: ["order_id", "product_id"]
  attributes:
    - name: "order_id"
      type: "int"
      foreignKey: "orders.order_id"
    - name: "product_id"
      type: "int"
      foreignKey: "products.product_id"
    - name: "quantity"
      type: "int"
    - name: "sale_price"
      type: "double"
```

## Examples

### Generate Full ER Diagram as SVG

```bash
starlake table-dependencies --svg --output tables.svg --all
```

### Generate Diagram for Specific Tables

```bash
starlake table-dependencies --tables starbake.orders,starbake.order_lines --svg --output orders.svg
```

### Show Only Related Tables

```bash
starlake table-dependencies --related --svg --output related.svg
```

### Generate JSON Output

```bash
starlake table-dependencies --json --output tables.json
```

### Show Only Primary/Foreign Keys

```bash
starlake table-dependencies --all-attrs false --svg --output keys_only.svg
```

### Generate PNG Image

```bash
starlake table-dependencies --png --output tables.png --all
```

## Related Skills

- [lineage](../lineage/SKILL.md) - Task dependency lineage graph
- [col-lineage](../col-lineage/SKILL.md) - Column-level lineage
- [acl-dependencies](../acl-dependencies/SKILL.md) - ACL dependency graph