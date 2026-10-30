-- Test: DataCleansing macro with Remove unwanted characters -> Letters
-- This test validates that letters are removed while digits and spaces are kept,
-- NULL stays NULL, and a column not selected for cleaning passes through unchanged.
-- relation_name is passed as a list, the same way the DataCleansing gem passes it.

WITH test_data AS (
    -- Create sample test data with numbers inside free text
    SELECT 1 AS id, '12 Months' AS duration, 'Plan A' AS label
    UNION ALL
    SELECT 2 AS id, '12Months' AS duration, 'Plan B' AS label
    UNION ALL
    SELECT 3 AS id, '24 months' AS duration, 'Plan C' AS label
    UNION ALL
    SELECT 4 AS id, NULL AS duration, NULL AS label  -- NULL values
),

expected_result AS (
    -- Expected: only the number (and any space) is left in duration; label is unchanged
    SELECT 1 AS id, '12 ' AS duration, 'Plan A' AS label
    UNION ALL
    SELECT 2 AS id, '12' AS duration, 'Plan B' AS label
    UNION ALL
    SELECT 3 AS id, '24 ' AS duration, 'Plan C' AS label
    UNION ALL
    SELECT 4 AS id, NULL AS duration, NULL AS label
),

actual_result AS (
    -- Call the DataCleansing macro to remove letters from duration
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "duration", "dataType": "String"}, {"name": "label", "dataType": "String"}],
        modifyCase='keepOriginal',
        columnNames=['duration'],
        cleanLetters=true
    ) }}
)

-- Test passes if every row matches (returns 0 rows)
-- Test fails if any value differs or a row is missing (returns rows)
SELECT
    'DataCleansing remove letters test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.duration AS expected_duration,
    actual.duration AS actual_duration,
    expected.label AS expected_label,
    actual.label AS actual_label
FROM expected_result expected
FULL OUTER JOIN actual_result actual ON expected.id = actual.id
WHERE expected.duration IS DISTINCT FROM actual.duration
   OR expected.label IS DISTINCT FROM actual.label
   OR expected.id IS NULL
   OR actual.id IS NULL
