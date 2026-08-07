-- Test: FindDuplicates macro - find unique records (Snowflake)
-- Validates that output_type="unique" returns exactly one row per group (row_num = 1)
-- Uses LocalStack Snowflake emulator for CI/CD
--
-- Source data: 6 rows across 3 groups (by ID, NAME)
--   Group (1, Alice): 2 rows
--   Group (2, Bob):   1 row
--   Group (3, Charlie): 3 rows
-- Expected: 3 unique rows (first from each group)

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE test_find_dup_unique_src AS
SELECT 1 AS ID, 'Alice' AS NAME, 100 AS AMOUNT
UNION ALL SELECT 1 AS ID, 'Alice' AS NAME, 100 AS AMOUNT
UNION ALL SELECT 2 AS ID, 'Bob' AS NAME, 200 AS AMOUNT
UNION ALL SELECT 3 AS ID, 'Charlie' AS NAME, 300 AS AMOUNT
UNION ALL SELECT 3 AS ID, 'Charlie' AS NAME, 300 AS AMOUNT
UNION ALL SELECT 3 AS ID, 'Charlie' AS NAME, 300 AS AMOUNT
{% endset %}
{% do run_query(create_src) %}

{% set macro_query %}
{{ prophecy_basics.FindDuplicates(
    'test_find_dup_unique_src',
    ['ID', 'NAME'],
    'equal_to',
    'unique',
    1, 0, 0,
    'selectedCols',
    ['ID', 'NAME', 'AMOUNT'],
    [{'expression': {'expression': 'ID'}, 'sortType': 'asc'}]
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set row_count = results | length %}
{% if row_count != 3 %}
    {{ exceptions.raise_compiler_error("FindDuplicates unique test FAILED: expected 3 unique rows, got " ~ row_count) }}
{% endif %}
{% endif %}

SELECT 1 WHERE 1=0
