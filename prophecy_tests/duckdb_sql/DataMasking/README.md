# DataMasking Macro Tests (DuckDB)

dbt tests for the `DataMasking` macro (`macros/DataMasking.sql`), covering every
masking method the gem offers on DuckDB through the `duckdb__` adapter.

The gem offers `sha`, `sha2` (256 bits only), `md5` and `mask` on DuckDB, so `hash`,
`crc32` and SHA2 224/384/512 are not covered.

## Test Files

1. **`test_mask_method.sql`** - character masking
- default substitutes, `"NULL"` to keep a category, custom substitutes
- other-character substitute, including spaces (matches Databricks `mask()`)
- several columns masked at once
2. **`test_hash_methods.sql`** - one-way hash methods
- `sha`, `sha2` 256 and `md5`, compared with Python `hashlib` digests
- `sha2` on an integer column (hashes its text form)
- several columns masked at once
3. **`test_output_columns.sql`** - which columns come back and in what order
- in place, prefix and suffix

## How These Tests Work

Each test:
1. Creates a temporary source table (a data row plus an all-NULL row)
2. Calls the `DataMasking` macro once per case
3. Compares the listed columns, ordered by id, with the expected values
4. Raises a compiler error naming the case when a value or column list differs

**Tests pass when they return 0 rows** and raise no error.

Every case also checks that NULL input stays NULL.

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
