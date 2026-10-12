# DataMasking Macro Tests (Snowflake)

dbt tests for the `DataMasking` macro (`macros/DataMasking.sql`), covering every
masking method the gem offers on Snowflake through the `snowflake__` adapter.

The Snowflake other-character pattern keeps whitespace, unlike Databricks `mask()`,
so the other-character case uses a value without spaces.

## Test Files

1. **`test_mask_method.sql`** - character masking
- default substitutes, `"NULL"` to keep a category, custom substitutes
- other-character substitute
- several columns masked at once
2. **`test_hash_methods.sql`** - one-way hash methods
- `sha`, `sha2` 224/256/384/512 and `md5`, compared with Python `hashlib` digests
- `sha2` on an integer column (hashes its text form)
- `hash` in place and combined, compared with `HASH()` called directly
- several columns masked at once
3. **`test_output_columns.sql`** - which columns come back and in what order
- in place, prefix, suffix and combined hash

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
