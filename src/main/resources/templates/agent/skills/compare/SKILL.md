---
name: compare
description: Compare two versions of a Starlake project
---

# Compare Skill

Compares two versions of a Starlake project and generates a diff report. Supports comparing by file paths, git commit SHAs, or git tags. Useful for reviewing schema changes, added/removed tables, and modified configurations between releases.

## Usage

```bash
starlake compare [options]
```

## Options

### By File Path
- `--path1 <value>`: Path to the old version of the project
- `--path2 <value>`: Path to the new version of the project

### By Git Commit
- `--gitWorkTree <value>`: Local path to the git repository
- `--commit1 <value>`: Old commit SHA
- `--commit2 <value>`: New commit SHA

### By Git Tag
- `--tag1 <value>`: Old git tag (use `latest` for most recent tag)
- `--tag2 <value>`: New git tag (use `latest` for most recent tag)

### Output
- `--template <value>`: SSP/Mustache template path for custom report format
- `--output <value>`: Output file path
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Examples

### Compare Git Commits

```bash
starlake compare --gitWorkTree /path/to/repo --commit1 abc1234 --commit2 def5678
```

### Compare HEAD with Previous Commit

```bash
starlake compare --gitWorkTree . --commit1 HEAD~1 --commit2 HEAD
```

### Compare Git Tags

```bash
starlake compare --gitWorkTree . --tag1 v1.0.0 --tag2 v2.0.0
```

### Compare with Latest Tag

```bash
starlake compare --gitWorkTree . --tag1 latest --commit2 HEAD
```

### Compare Two Directory Paths

```bash
starlake compare --path1 /old/project --path2 /new/project
```

### Compare with Custom Template

```bash
starlake compare --gitWorkTree . --commit1 HEAD~1 --commit2 HEAD --template /path/to/report.ssp --output /tmp/report.html
```

## Related Skills

- [migrate](../migrate/SKILL.md) - Migrate project to latest version
- [validate](../validate/SKILL.md) - Validate project configuration