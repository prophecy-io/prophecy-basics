# CountRecords Macro Tests

This directory contains dbt tests for the `CountRecords` macro (`macros/CountRecords.sql`).

## Test Files

1. **`test_count_records.sql`** - Tests total record count
2. **`test_count_non_null_records.sql`** - Tests non-null counts per column
3. **`test_count_distinct_records.sql`** - Tests distinct counts per column

## How These Tests Work

Each test:
1. Creates inline test data using CTEs
2. Defines expected results
3. Calls the `CountRecords` macro
4. Returns rows only if actual ≠ expected

**Tests pass when they return 0 rows** (meaning no discrepancies).

## Example Test Structure

```sql
WITH test_data AS (
    -- Inline test data
    SELECT 1 AS id, 'Alice' AS name
),
expected_result AS (
    -- What we expect
    SELECT 1 AS total_records
),
actual_result AS (
    -- Call the macro
    SELECT {{ prophecy_basics.CountRecords('test_data', [], 'count_total_records') }} AS total_records
)
-- Return rows only if there's a mismatch
SELECT * FROM actual_result
WHERE total_records != (SELECT total_records FROM expected_result)
```

## Running These Tests

**⚠️ IMPORTANT**: Before running tests locally, copy all test folders to `tests/` directory:

```bash
# From project root (excludes venv to avoid conflicts)
mkdir -p tests/duckdb_sql
find prophecy_tests/duckdb_sql -mindepth 1 -maxdepth 1 -type d ! -name "venv" -exec cp -r {} tests/duckdb_sql/ \;

# Then run tests
cd prophecy_tests/duckdb_sql
source venv/bin/activate
dbt test --project-dir ../.. --profiles-dir .
```

See parent README (`../README.md`) for full setup instructions.

## Status

✅ All 3 tests passing

