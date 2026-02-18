---
name: serve
description: Run the Starlake HTTP server
---

# Serve Skill

Starts the Starlake HTTP server, which provides a REST API for running commands programmatically. The server supports all Starlake commands via HTTP endpoints.

## Usage

```bash
starlake serve [options]
```

## Options

- `--host <value>`: Address on which the server listens (default: `localhost`)
- `--port <value>`: Port on which the server listens (default: `11000`)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Environment Variables

- `SL_API_HTTP_PORT`: Override the default server port
- `SL_API_DOMAIN`: Override the default server host/IP

## Examples

### Start Server on Default Port

```bash
starlake serve
```

### Start Server on Custom Port

```bash
starlake serve --port 8080
```

### Start Server on All Interfaces

```bash
starlake serve --host 0.0.0.0 --port 8080
```

## Related Skills

- [console](../console/SKILL.md) - Interactive REPL console
- [config](../config/SKILL.md) - Configuration reference