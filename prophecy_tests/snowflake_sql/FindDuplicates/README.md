# FindDuplicates Macro Tests (Snowflake)

This directory contains dbt singular tests for the `FindDuplicates` macro (`macros/FindDuplicates.sql`) targeting the Snowflake adapter.

## Test Files

1. **`test_find_unique_records.sql`** - Tests `output_type="unique"` (returns first row per group)
2. **`test_find_duplicate_records.sql`** - Tests `output_type="duplicate"` (returns non-first rows)
3. **`test_find_group_count.sql`** - Tests `output_type="custom_group_count"` with `greater_than` filter

## How These Tests Work

Each test uses compile-time assertions via `run_query` + `raise_compiler_error`:
1. Creates a temporary table with inline test data
2. Calls the `FindDuplicates` macro to generate SQL
3. Executes the macro SQL via `run_query` (as a top-level query)
4. Counts results and raises a compiler error if the count doesn't match expected

**Tests pass when no compiler error is raised.**

## Running These Tests

### Prerequisites

- A Snowflake account with credentials (account, user, password, database, warehouse)

### Local Testing

```bash
# From project root

# Step 1: Set Snowflake credentials
export SNOWFLAKE_ACCOUNT="your-account-id"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_DATABASE="your-database"
export SNOWFLAKE_WAREHOUSE="your-warehouse"

# Step 2: Copy test folders to tests/ directory
mkdir -p tests/snowflake_sql
find prophecy_tests/snowflake_sql -mindepth 1 -maxdepth 1 -type d ! -name "venv" \
  -exec cp -r {} tests/snowflake_sql/ \;

# Step 3: Setup environment
cd prophecy_tests/snowflake_sql
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Step 4: Run tests
dbt test --project-dir ../.. --profiles-dir .
```

### CI/CD (GitHub Actions)

Credentials can be provided in two ways:
1. **Manual run**: Fill in the credential fields in the "Run workflow" dialog
2. **Automated run**: Configure repo secrets (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, etc.)

## Status

Active - 3 tests for FindDuplicates macro
