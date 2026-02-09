---
name: stage
description: Move files from landing to pending area
---

# Stage Skill

This skill moves files from the landing area to the pending area (staging).
It handles decompression and ACK file checking.

## Usage

```bash
starlake stage [options]
```

## Options

- `--domains <value>`: Domains to stage
- `--tables <value>`: Tables to stage
- `--options <value>`: Stage arguments to be used as substitutions

## Examples

### Stage All

```bash
starlake stage
```
