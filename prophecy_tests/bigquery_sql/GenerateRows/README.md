# GenerateRows Macro Tests (BigQuery)

dbt tests for the `GenerateRows` macro (`macros/GenerateRows.sql`) on BigQuery, through the
`bigquery__` adapter.

BigQuery only accepts `WITH RECURSIVE` at the top level of a statement, and Prophecy nests
gem SQL inside a CTE, so this adapter does not recurse: it computes the value at each offset
(`init + k * step`, or `DATE_ADD(init, INTERVAL k * step part)`) and keeps the offsets
before the first one that fails the condition. The loop expression therefore needs a
constant step; anything else is a compile error.

## Test Files

1. **`test_generate_from_source.sql`** - one sequence per source row
- order quantities expanded into one row per unit; a quantity of 0 yields no rows
- weekly billing dates from `payload.start_date` through `payload.end_date`
- a repayment schedule whose generated `balance` replaces the source `balance` column,
  stepping by a payload column
- monthly dates from a month-end start stay anchored to it (Jan 31, Feb 29, Mar 31, Apr 30)
2. **`test_generate_sequence.sql`** - sequences without a source table
- a counter from 1 to 10
- `max_rows` cutting off a condition that never fails
- business days of a week: generation stops at the first Saturday and does not resume

## How These Tests Work

Each case embeds the macro in a CTE, as Prophecy-generated models do, runs it, and compares
the output column list and the ordered values (as text) with the expected result. A
mismatch raises a compiler error naming the case.

**Tests pass when they return 0 rows** and raise no error.

Every call passes `max_rows` as a string and keeps quotes out of the expressions, so it
also round-trips through the code-to-visual tester.

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
