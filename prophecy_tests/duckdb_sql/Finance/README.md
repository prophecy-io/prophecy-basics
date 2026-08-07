# Finance Macro Tests (DuckDB)

dbt tests for the `Finance` macro (`macros/Finance.sql`), covering the 12
closed-form finance functions on DuckDB.

Runs on DuckDB; the duckdb__ adapter emits date_diff('day', ...) and SELECT * EXCLUDE.

## Test Files

1. **`test_time_value_of_money.sql`** - FV, PV, PMT, NPER
2. **`test_rate_conversions.sql`** - CAGR, EffectiveRate, NominalRate, FVSchedule
3. **`test_cashflow_discounting.sql`** - NPV, XNPV, MIRR, MXIRR

IRR, XIRR and Rate are not covered here. DuckDB itself runs them in milliseconds
because it keeps the per-iteration CTEs as separate operators, but Databricks and
Snowflake inline them and hang, so a green DuckDB result here would be misleading
rather than reassuring. Those three functions are covered by the Spark tests in
`prophecy_tests/python_gems/Finance/`, which loop in Python instead.

## How These Tests Work

Each test:
1. Creates a temporary source table holding one row of inputs
2. Calls the `Finance` macro once per function
3. Reads the appended result column and compares it to the value Excel produces
4. Raises a compiler error naming the function when a result is out of tolerance

**Tests pass when they return 0 rows** and raise no error.

Expected values come from Excel's finance functions, so a failure means the macro
drifted away from the behaviour users expect rather than away from a previous run.

## Sign Convention

Cash leaving your pocket is negative and cash arriving is positive, matching Excel.
`FV(rate=0.05, nper=10, pmt=-100, pv=-1000)` is 2886.68 because both the initial
1000 and the ten payments of 100 are outflows that grow into a positive balance.

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
