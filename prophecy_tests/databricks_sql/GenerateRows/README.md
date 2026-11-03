# GenerateRows Macro Tests (Databricks)

dbt tests for the `GenerateRows` macro (`macros/GenerateRows.sql`) on Databricks, through the
recursive `default__` adapter.

Recursive CTEs need Databricks Runtime 17.0+ (or a current SQL warehouse) and stop at 100
recursion levels by default, so every sequence here stays under 100 rows.

## Test Files

1. **`test_generate_from_source.sql`** - one sequence per source row
- order quantities expanded into one row per unit; a quantity of 0 yields no rows
- weekly billing dates from `payload.start_date` through `payload.end_date`
- a repayment schedule whose generated `balance` replaces the source `balance` column
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
mkdir -p tests/databricks_sql
find prophecy_tests/databricks_sql -mindepth 1 -maxdepth 1 -type d ! -name "venv" -exec cp -r {} tests/databricks_sql/ \;

# Then run tests
cd prophecy_tests/databricks_sql
source venv/bin/activate
dbt test --project-dir ../.. --profiles-dir .
```

See parent README (`../README.md`) for full setup instructions.

## Status

⚠️ Not yet run against a live Databricks warehouse
