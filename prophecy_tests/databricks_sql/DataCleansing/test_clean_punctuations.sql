-- Test: DataCleansing macro with Remove unwanted characters -> Punctuations (Databricks SQL)
-- This test validates that punctuation is removed while letters, digits and spaces are kept,
-- and NULL stays NULL.

WITH test_data AS (
    -- Create sample test data with punctuation
    SELECT 1 AS id, 'Hello, World!' AS label
    UNION ALL
    SELECT 2 AS id, 'Total: $1,200.50' AS label
    UNION ALL
    SELECT 3 AS id, 'A-1.5' AS label
    UNION ALL
    SELECT 4 AS id, NULL AS label  -- NULL value
),

expected_result AS (
    -- Expected: punctuation removed, everything else unchanged
    SELECT 1 AS id, 'Hello World' AS label
    UNION ALL
    SELECT 2 AS id, 'Total 120050' AS label
    UNION ALL
    SELECT 3 AS id, 'A15' AS label
    UNION ALL
    SELECT 4 AS id, NULL AS label
),

actual_result AS (
    -- Call the DataCleansing macro to remove punctuation from label
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "label", "dataType": "String"}],
        modifyCase='keepOriginal',
        columnNames=['label'],
        cleanPunctuations=true
    ) }}
)

-- Test passes if every row matches (returns 0 rows)
-- Test fails if any value differs or a row is missing (returns rows)
SELECT
    'DataCleansing remove punctuations test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.label AS expected_label,
    actual.label AS actual_label
FROM expected_result expected
FULL OUTER JOIN actual_result actual ON expected.id = actual.id
WHERE expected.label IS DISTINCT FROM actual.label
   OR expected.id IS NULL
   OR actual.id IS NULL
