---
name: job
description: Run a job (alias for transform)
---

# Job Skill

Alias for the [transform](../transform/SKILL.md) command. All options and behavior are identical.

## Usage

```bash
starlake job [options]
```

## Options

See [transform](../transform/SKILL.md) for all available options.

## Examples

### Run a Job

```bash
starlake job --name kpi.revenue_summary
```

### Run with Recursive Dependencies

```bash
starlake job --name kpi.order_summary --recursive
```

## Related Skills

- [transform](../transform/SKILL.md) - Full documentation for this command