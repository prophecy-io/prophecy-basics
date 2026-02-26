-- Test: FindDuplicates macro - filter by group count (Snowflake)
-- Validates that output_type="custom_group_count" with condition="greater_than" and value=1
-- returns ALL rows from groups whose count exceeds the threshold
-- Uses LocalStack Snowflake emulator for CI/CD
--
-- Source data: 6 rows across 3 groups (by ID, NAME)
--   Group (1, Alice):   count=2 -> included (2 > 1)
--   Group (2, Bob):     count=1 -> excluded (1 is not > 1)
--   Group (3, Charlie): count=3 -> included (3 > 1)
-- Expected: 5 rows (2 from Alice group + 3 from Charlie group)

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE test_find_dup_grp_src AS
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
    'test_find_dup_grp_src',
    ['ID', 'NAME'],
    'greater_than',
    'custom_group_count',
    1, 0, 0,
    'selectedCols',
    ['ID', 'NAME', 'AMOUNT'],
    [{'expression': {'expression': 'ID'}, 'sortType': 'asc'}]
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set row_count = results | length %}
{% if row_count != 5 %}
    {{ exceptions.raise_compiler_error("FindDuplicates group count test FAILED: expected 5 rows from groups with count > 1, got " ~ row_count) }}
{% endif %}
{% endif %}

SELECT 1 WHERE 1=0
