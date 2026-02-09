---
name: compare
description: Compare two Starlake project versions
---

# Compare Skill

This skill compares two versions of a Starlake project (e.g., git commits).

## Usage

```bash
starlake compare [options]
```

## Options

- `--path1 <value>`: Old version path
- `--path2 <value>`: New version path
- `--gitWorkTree <value>`: Local git project path
- `--commit1 <value>`: Old commit SHA
- `--commit2 <value>`: New commit SHA
- `--tag1 <value>`: Old tag
- `--tag2 <value>`: New tag
- `--template <value>`: Template path
- `--output <value>`: Output path

## Examples

### Compare Commits

```bash
starlake compare --gitWorkTree /path/to/repo --commit1 HEAD~1 --commit2 HEAD
```
