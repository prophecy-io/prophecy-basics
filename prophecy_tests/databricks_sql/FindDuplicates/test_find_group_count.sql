-- Test: FindDuplicates macro - filter by group count (Databricks SQL)
-- Validates that output_type="custom_group_count" with condition="greater_than" and value=1
-- returns ALL rows from groups whose count exceeds the threshold
-- Requires a real Databricks cluster (uses Databricks-specific SELECT * EXCEPT syntax)
--
-- Source data: 6 rows across 3 groups (by id, name)
--   Group (1, Alice):   count=2 -> included (2 > 1)
--   Group (2, Bob):     count=1 -> excluded (1 is not > 1)
--   Group (3, Charlie): count=3 -> included (3 > 1)
-- Expected: 5 rows (2 from Alice group + 3 from Charlie group)

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY VIEW test_find_dup_grp_src AS
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
    'test_find_dup_grp_src',
    ['id', 'name'],
    'greater_than',
    'custom_group_count',
    1, 0, 0,
    'selectedCols',
    ['id', 'name', 'amount'],
    [{'expression': {'expression': 'id'}, 'sortType': 'asc'}]
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set row_count = results | length %}
{% if row_count != 5 %}
    {{ exceptions.raise_compiler_error("FindDuplicates group count test FAILED: expected 5 rows from groups with count > 1, got " ~ row_count) }}
{% endif %}
{% endif %}

SELECT 1 WHERE 1=0
