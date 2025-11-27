# DuckDB SQL Tests

This directory contains dbt unit tests for DuckDB SQL macros and functionality.

## Test Approach

These tests use **dbt unit tests** with DuckDB in-memory database to validate SQL macros, rather than pytest. The tests are defined in the main dbt project using dbt's unit testing framework.

## Setup

The test runner automatically sets up the environment, but for manual setup:

```bash
cd prophecy_tests/duckdb_sql

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dbt and dependencies
pip install -r requirements.txt
```

## Configuration

### profiles.yml

The `profiles.yml` file configures DuckDB with in-memory database:

```yaml
type: duckdb
path: ':memory:'  # In-memory database for fast tests
```

This allows tests to run quickly without any external dependencies.

## Running Tests

### Using Test Runner (Recommended)

```bash
cd prophecy_tests
python run_tests.py duckdb_sql
```

### Manual dbt Commands

```bash
# From project root
dbt test --select test_type:unit
```

## Writing Unit Tests

dbt unit tests are defined in YAML files. Example:

```yaml
# models/schema.yml
unit_tests:
  - name: test_duckdb_macro
    model: my_model
    given:
      - input: ref('source_table')
        rows:
          - {id: 1, name: 'alice'}
    expect:
      rows:
        - {id: 1, name: 'ALICE'}
```

See [dbt unit testing docs](https://docs.getdbt.com/docs/build/unit-tests) for more details.

## Advantages of DuckDB for Testing

- 🚀 **Fast**: In-memory database for quick test execution
- 💾 **Lightweight**: No external database required
- 🔧 **Easy Setup**: Works out of the box
- 📊 **Full SQL Support**: Comprehensive SQL dialect support

## Status
⚠️ **Coming Soon** - Tests not yet implemented

