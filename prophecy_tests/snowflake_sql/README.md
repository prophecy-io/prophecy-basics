# Snowflake SQL Tests

This directory contains dbt unit tests for Snowflake SQL macros and functionality.

## Test Approach

These tests use **dbt unit tests** to validate SQL macros, rather than pytest. The tests are defined in the main dbt project using dbt's unit testing framework.

## Setup

The test runner automatically sets up the environment, but for manual setup:

```bash
cd prophecy_tests/snowflake_sql

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dbt and dependencies
pip install -r requirements.txt
```

## Configuration

### profiles.yml

The `profiles.yml` file configures the Snowflake connection. It uses environment variables for credentials:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`

Set these in your environment or CI/CD secrets.

## Running Tests

### Using Test Runner (Recommended)

```bash
cd prophecy_tests
python run_tests.py snowflake_sql
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
  - name: test_my_macro
    model: my_model
    given:
      - input: ref('source_table')
        rows:
          - {id: 1, name: 'test'}
    expect:
      rows:
        - {id: 1, name: 'TEST'}
```

See [dbt unit testing docs](https://docs.getdbt.com/docs/build/unit-tests) for more details.

## Status
⚠️ **Coming Soon** - Tests not yet implemented

