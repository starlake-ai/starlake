---
name: preload
description: Check for files to load
---

# PreLoad Skill

This skill checks if files are available for loading in the landing area.

## Usage

```bash
starlake preload [options]
```

## Options

- `--domain <value>`: Domain to pre load (required)
- `--tables <value>`: Tables to pre load
- `--strategy <value>`: Pre load strategy (Imported, Pending, Ack)
- `--globalAckFilePath <value>`: Global ack file path
- `--options <value>`: Pre load arguments to be used as substitutions

## Examples

### Preload Check

```bash
starlake preload --domain mydomain
```
