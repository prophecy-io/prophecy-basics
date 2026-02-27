# Snowflake SQL Tests

This directory contains tests for Snowflake SQL macros.

## Test Approach

These tests will **call macros directly** from the `macros/` directory using dbt, without creating any models.

Example: Testing `macros/CountRecords.sql` directly with test data.

## Setup

```bash
cd prophecy_tests/snowflake_sql

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dbt and dependencies
pip install -r requirements.txt
```

## Configuration

The `profiles.yml` uses environment variables for Snowflake credentials:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`

Set these in your environment or CI/CD secrets.

## Running Tests

When tests are implemented, run with:

```bash
# From project root
dbt test
```

## Writing Tests

Tests will directly invoke macros like:

```sql
-- Example: Testing CountRecords macro
SELECT {{ CountRecords(ref('test_data')) }}
```

No models need to be created - just call the macros with test data.

## Status
⚠️ **Coming Soon** - Tests not yet implemented
