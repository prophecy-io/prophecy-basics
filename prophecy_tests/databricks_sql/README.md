# Databricks SQL Tests

This directory contains dbt unit tests for Databricks SQL macros and functionality.

## Test Approach

These tests use **dbt unit tests** with Spark session backend to validate SQL macros, rather than pytest. The tests are defined in the main dbt project using dbt's unit testing framework.

## Setup

The test runner automatically sets up the environment, but for manual setup:

```bash
cd prophecy_tests/databricks_sql

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dbt and dependencies
pip install -r requirements.txt
```

## Configuration

### profiles.yml

The `profiles.yml` file configures the Spark session for unit testing with in-memory catalog:

```yaml
type: spark
method: session
schema: testing
```

This allows tests to run locally without a Databricks cluster connection.

## Running Tests

### Using Test Runner (Recommended)

```bash
cd prophecy_tests
python run_tests.py databricks_sql
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
  - name: test_databricks_macro
    model: my_model
    given:
      - input: ref('source_table')
        rows:
          - {id: 1, value: 'test'}
    expect:
      rows:
        - {id: 1, value: 'TRANSFORMED'}
```

See [dbt unit testing docs](https://docs.getdbt.com/docs/build/unit-tests) for more details.

## Requirements

- Python 3.11+
- Java 11+ (for Spark session)

## Status
⚠️ **Coming Soon** - Tests not yet implemented

