---
name: test
description: Run integration tests for your Starlake project
---

# Test Skill

Runs integration tests for your Starlake project. Tests verify that load and transform tasks produce the expected output by comparing actual results against expected data files stored in `metadata/tests/`.

## Usage

```bash
starlake test [options]
```

## Options

- `--load`: Test load tasks only
- `--transform`: Test transform tasks only
- `--domain <value>`: Test only this domain
- `--table <value>`: Test only this table/task within the selected domain
- `--test <value>`: Run only this specific test case
- `--site`: Generate test results as a website
- `--outputDir <value>`: Output directory for test results
- `--accessToken <value>`: Access token for authentication (e.g. GCP)
- `--reportFormat <value>`: Report output format: `console`, `json`, or `html`

## Test Directory Structure

Tests are organized in `metadata/tests/`:

```
metadata/tests/
├── load/
│   └── {domain}/
│       └── {table}/
│           └── {test_name}/
│               ├── _expected.csv          # Expected output data
│               ├── _expected.json         # Or JSON format
│               └── _incoming.{file}       # Input test data
└── transform/
    └── {domain}/
        └── {task}/
            └── {test_name}/
                ├── _expected.csv          # Expected output
                └── {source_table}.csv     # Source table data
```

### Load Test Example

```
metadata/tests/load/starbake/orders/test1/
├── _expected.csv                          # Expected loaded data
└── _incoming.orders_20240301.json         # Input file to load
```

### Transform Test Example

```
metadata/tests/transform/kpi/revenue_summary/test1/
├── _expected.csv                          # Expected transform output
├── starbake.orders.csv                    # Mock source: orders table
└── starbake.order_lines.csv              # Mock source: order_lines table
```

## Examples

### Run All Tests

```bash
starlake test
```

### Run Load Tests Only

```bash
starlake test --load
```

### Run Transform Tests Only

```bash
starlake test --transform
```

### Run Tests for a Specific Domain

```bash
starlake test --domain starbake
```

### Run Tests for a Specific Table

```bash
starlake test --domain starbake --table orders
```

### Run a Specific Test Case

```bash
starlake test --domain starbake --table orders --test test1
```

### Generate Test Results Website

```bash
starlake test --site --outputDir /tmp/test-results
```

## Related Skills

- [load](../load/SKILL.md) - Load data (tested by load tests)
- [transform](../transform/SKILL.md) - Run transforms (tested by transform tests)
- [validate](../validate/SKILL.md) - Validate project configuration