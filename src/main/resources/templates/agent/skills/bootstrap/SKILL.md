---
name: bootstrap
description: Create a new project optionally based on a specific template
---

# Bootstrap Skill

This skill helps you bootstrap a new Starlake project.

## Usage

```bash
starlake bootstrap [options]
```

## Options

- `--template <value>`: Template to use (default: quickstart). Available templates include `quickstart`, `simple`, etc.
- `--no-exit`: Should the JVM exit after project creation? (Useful for testing)

## Examples

### Bootstrap Default (Quickstart)

Create a new Starlake project using the default quickstart template.

```bash
starlake bootstrap
```

### Bootstrap with Specific Template

Create a new Starlake project using a custom template.

```bash
starlake bootstrap --template simple
```
