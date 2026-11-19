# DataValidation Macro Tests (BigQuery)

dbt tests for the `DataValidation` macro (`macros/DataValidation.sql`) on BigQuery, through the
`bigquery__` adapter.

## Test Files

1. **`test_row_checks.sql`** - six row-level checks on an orders table
- passed rows come out with exactly the input columns
- failed rows list every check they failed in `dq_failed_checks`, an array of check names (custom
  names replace the default)
- all rows carry `dq_passed` and `dq_failed_checks`; passing rows have an empty `dq_failed_checks`
- disallowed values and a date range given as text bounds
- unique that keeps one row of each key (earliest `order_date`, then highest `amount`), with reason codes: custom
  and default codes in `dq_failed_codes`, and an empty list for passing rows
2. **`test_failure_handling.sql`** - what happens when checks fail
- summary: one row per check, including the row count and required columns checks, with a
  `reason_code` column
- errors: one row per failed check, with the number of failing rows and a message
- "Send no rows to Passed": the passed output is empty when a check fails, and passed rows
  flow through when the failing rows are within the allowed number
- "Stop the pipeline" with clean data: rows flow through unchanged
- "Stop the pipeline" with failing rows within the allowed number: rows flow through
- "Stop the pipeline" with an empty input and no failures: no rows, no error

## How These Tests Work

Each case embeds the macro in a CTE, as Prophecy-generated models do, runs it, and compares
the output column list and the ordered values (as text) with the expected result. A
mismatch raises a compiler error naming the case.

**Tests pass when they return 0 rows** and raise no error.

A dbt test cannot catch a query that stops with an error, so the failing side of "Stop the
pipeline" is not in these files. It was checked by hand on DuckDB: one failing row, more
failing rows than allowed, every row failing (so no row passes), a filter after the gem, and
an empty input below the minimum row count all stop the query with a
`DataValidation failed: ...` message. A missing required column, or a value range that
compares a text column with a number, stops it at compile time.

Every call uses the argument format the gem generates, so it also round-trips through the
code-to-visual tester.

## Running These Tests

**⚠️ IMPORTANT**: Before running tests locally, copy all test folders to `tests/` directory:

```bash
# From project root (excludes venv to avoid conflicts)
mkdir -p tests/bigquery_sql
find prophecy_tests/bigquery_sql -mindepth 1 -maxdepth 1 -type d ! -name "venv" -exec cp -r {} tests/bigquery_sql/ \;

# Then run tests
cd prophecy_tests/bigquery_sql
source venv/bin/activate
dbt test --project-dir ../.. --profiles-dir .
```

See parent README (`../README.md`) for full setup instructions.

## Status

⚠️ Not yet run against a live BigQuery project
