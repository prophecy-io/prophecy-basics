-- Test: FindDuplicates macro - find unique records (DuckDB)
-- Validates that output_type="unique" returns exactly one row per group (row_num = 1)
--
-- Source data: 6 rows across 3 groups (by id, name)
--   Group (1, Alice): 2 rows
--   Group (2, Bob):   1 row
--   Group (3, Charlie): 3 rows
-- Expected: 3 unique rows (first from each group)

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE test_find_dup_unique_src AS
SELECT 1 AS id, 'Alice' AS name, 100 AS amount
UNION ALL SELECT 1 AS id, 'Alice' AS name, 100 AS amount
UNION ALL SELECT 2 AS id, 'Bob' AS name, 200 AS amount
UNION ALL SELECT 3 AS id, 'Charlie' AS name, 300 AS amount
UNION ALL SELECT 3 AS id, 'Charlie' AS name, 300 AS amount
UNION ALL SELECT 3 AS id, 'Charlie' AS name, 300 AS amount
{% endset %}
{% do run_query(create_src) %}

{% set macro_query %}
{{ prophecy_basics.FindDuplicates(
    'test_find_dup_unique_src',
    ['id', 'name'],
    'equal_to',
    'unique',
    1, 0, 0,
    'selectedCols',
    ['id', 'name', 'amount'],
    [{'expression': {'expression': 'id'}, 'sortType': 'asc'}]
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set row_count = results | length %}
{% if row_count != 3 %}
    {{ exceptions.raise_compiler_error("FindDuplicates unique test FAILED: expected 3 unique rows, got " ~ row_count) }}
{% endif %}
{% endif %}

SELECT 1 WHERE 1=0
