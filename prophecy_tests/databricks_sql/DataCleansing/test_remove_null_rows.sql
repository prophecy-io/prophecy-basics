-- Test: DataCleansing macro with Remove rows with null in every column (Databricks SQL)
-- This test validates that only rows where every column is NULL are removed,
-- and that rows with some NULLs are kept unchanged.
-- No columns are selected for cleaning, so the macro returns all columns as they are.

WITH test_data AS (
    -- Create sample test data with partly NULL and fully NULL rows
    SELECT 1 AS id, 'Alice' AS name, 10 AS qty
    UNION ALL
    SELECT 2 AS id, NULL AS name, 20 AS qty  -- some NULLs: kept
    UNION ALL
    SELECT NULL AS id, NULL AS name, NULL AS qty  -- every column NULL: removed
    UNION ALL
    SELECT 4 AS id, NULL AS name, NULL AS qty  -- only id set: kept
),

expected_result AS (
    -- Expected: the fully NULL row is removed, the rest are unchanged
    SELECT 1 AS id, 'Alice' AS name, 10 AS qty
    UNION ALL
    SELECT 2 AS id, NULL AS name, 20 AS qty
    UNION ALL
    SELECT 4 AS id, NULL AS name, NULL AS qty
),

actual_result AS (
    -- Call the DataCleansing macro to remove fully NULL rows
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "name", "dataType": "String"}, {"name": "qty", "dataType": "Integer"}],
        modifyCase='keepOriginal',
        columnNames=[],
        removeRowNullAllCols=true
    ) }}
)

-- Test passes if every row matches (returns 0 rows)
-- Test fails if any value differs or a row is missing or extra (returns rows)
SELECT
    'DataCleansing remove null rows test failed' AS test_name,
    expected.id AS expected_id,
    actual.id AS actual_id
FROM expected_result expected
FULL OUTER JOIN actual_result actual ON expected.id = actual.id
WHERE expected.name IS DISTINCT FROM actual.name
   OR expected.qty IS DISTINCT FROM actual.qty
   OR expected.id IS NULL
   OR actual.id IS NULL
