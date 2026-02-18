---
name: console
description: Start the Starlake interactive REPL console
---

# Console Skill

Starts the Starlake REPL (Read-Evaluate-Print Loop) console for interactive command execution. Allows you to run Starlake commands interactively without restarting the JVM each time.

## Usage

```bash
starlake console
```

## Options

- `--options k1=v1,k2=v2`: Additional options (currently ignored)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Start Console

```bash
starlake console
```

## Related Skills

- [serve](../serve/SKILL.md) - Run as HTTP server instead of interactive console