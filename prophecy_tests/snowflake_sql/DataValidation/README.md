# DataValidation Macro Tests (Snowflake)

dbt tests for the `DataValidation` macro (`macros/DataValidation.sql`) on Snowflake, through the
`snowflake__` adapter.

Columns are upper case, as Prophecy reports unquoted Snowflake columns, so default check
names (for example `Not null: ORDER_ID, CUSTOMER_EMAIL`) and result columns are upper case too.

## Test Files

1. **`test_row_checks.sql`** - six row-level checks on an orders table
- passed rows come out with exactly the input columns
- failed rows list every check they failed in `dq_failed_checks` (custom names replace the default)
- all rows carry `dq_passed` and `dq_failed_checks`; passing rows have a NULL `dq_failed_checks`
- disallowed values and a date range given as text bounds
2. **`test_failure_handling.sql`** - what happens when checks fail
- summary: one row per check, including the row count and required columns checks
- "Stop the pipeline" with clean data: rows flow through unchanged

## How These Tests Work

Each case embeds the macro in a CTE, as Prophecy-generated models do, runs it, and compares
the output column list and the ordered values (as text) with the expected result. A
mismatch raises a compiler error naming the case.

**Tests pass when they return 0 rows** and raise no error.

A dbt test cannot catch a query that stops with an error, so the failing side of "Stop the
pipeline" is not in these files. It was checked by hand on DuckDB: failing rows, an empty
input with a minimum row count, and a downstream filter that removes every row all stop the
query with a `DataValidation failed: ...` message. A missing required column stops it at
compile time.

Every call uses the argument format the gem generates, so it also round-trips through the
code-to-visual tester.

## Running These Tests

**⚠️ IMPORTANT**: Before running tests locally, copy all test folders to `tests/` directory:

```bash
# From project root (excludes venv to avoid conflicts)
mkdir -p tests/snowflake_sql
find prophecy_tests/snowflake_sql -mindepth 1 -maxdepth 1 -type d ! -name "venv" -exec cp -r {} tests/snowflake_sql/ \;

# Then run tests
cd prophecy_tests/snowflake_sql
source venv/bin/activate
dbt test --project-dir ../.. --profiles-dir .
```

See parent README (`../README.md`) for full setup instructions.

## Status

⚠️ Not yet run against a live Snowflake account
