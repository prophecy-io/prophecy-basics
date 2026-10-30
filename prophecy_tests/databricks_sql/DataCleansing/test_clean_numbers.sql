-- Test: DataCleansing macro with Remove unwanted characters -> Numbers (Databricks SQL)
-- This test validates that digits are removed while letters and spaces are kept,
-- and NULL stays NULL.
-- relation_name is passed as a list, the same way the DataCleansing gem passes it.

WITH test_data AS (
    -- Create sample test data with numbers inside free text
    SELECT 1 AS id, '12 Months' AS duration
    UNION ALL
    SELECT 2 AS id, '12Months' AS duration
    UNION ALL
    SELECT 3 AS id, '24 months' AS duration
    UNION ALL
    SELECT 4 AS id, NULL AS duration  -- NULL value
),

expected_result AS (
    -- Expected: only the word (and any space) is left in duration
    SELECT 1 AS id, ' Months' AS duration
    UNION ALL
    SELECT 2 AS id, 'Months' AS duration
    UNION ALL
    SELECT 3 AS id, ' months' AS duration
    UNION ALL
    SELECT 4 AS id, NULL AS duration
),

actual_result AS (
    -- Call the DataCleansing macro to remove numbers from duration
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "duration", "dataType": "String"}],
        modifyCase='keepOriginal',
        columnNames=['duration'],
        cleanNumbers=true
    ) }}
)

-- Test passes if every row matches (returns 0 rows)
-- Test fails if any value differs or a row is missing (returns rows)
SELECT
    'DataCleansing remove numbers test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.duration AS expected_duration,
    actual.duration AS actual_duration
FROM expected_result expected
FULL OUTER JOIN actual_result actual ON expected.id = actual.id
WHERE expected.duration IS DISTINCT FROM actual.duration
   OR expected.id IS NULL
   OR actual.id IS NULL
