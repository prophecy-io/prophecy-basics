-- Test: DataCleansing macro with the whitespace options (Databricks SQL)
-- This test validates each whitespace option on its own:
--   Leading and trailing whitespace: removes outer spaces, inner whitespace is kept
--   Tabs, line breaks and duplicate whitespace: each run of whitespace becomes one space
--   All whitespace: every space, tab and line break is removed
-- NULL stays NULL for every option.

WITH test_data AS (
    -- Create sample test data with outer spaces, tabs, line breaks and repeated spaces
    SELECT 1 AS id, '  12 Months  ' AS duration
    UNION ALL
    SELECT 2 AS id, 'a' || CHR(9) || 'b' || CHR(10) || 'c' AS duration  -- tab and line break
    UNION ALL
    SELECT 3 AS id, 'a   b' AS duration  -- repeated spaces
    UNION ALL
    SELECT 4 AS id, NULL AS duration  -- NULL value
),

expected_result AS (
    -- Expected result for each option
    SELECT 1 AS id, '12 Months' AS trimmed, ' 12 Months ' AS collapsed, '12Months' AS no_whitespace
    UNION ALL
    SELECT 2 AS id, 'a' || CHR(9) || 'b' || CHR(10) || 'c' AS trimmed, 'a b c' AS collapsed, 'abc' AS no_whitespace
    UNION ALL
    SELECT 3 AS id, 'a   b' AS trimmed, 'a b' AS collapsed, 'ab' AS no_whitespace
    UNION ALL
    SELECT 4 AS id, NULL AS trimmed, NULL AS collapsed, NULL AS no_whitespace
),

actual_trimmed AS (
    -- Leading and trailing whitespace
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "duration", "dataType": "String"}],
        modifyCase='keepOriginal',
        columnNames=['duration'],
        trimWhiteSpace=true
    ) }}
),

actual_collapsed AS (
    -- Tabs, line breaks and duplicate whitespace
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "duration", "dataType": "String"}],
        modifyCase='keepOriginal',
        columnNames=['duration'],
        removeTabsLineBreaksAndDuplicateWhitespace=true
    ) }}
),

actual_no_whitespace AS (
    -- All whitespace
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "duration", "dataType": "String"}],
        modifyCase='keepOriginal',
        columnNames=['duration'],
        allWhiteSpace=true
    ) }}
)

-- Test passes if every row matches (returns 0 rows)
-- Test fails if any value differs or a row is missing (returns rows)
SELECT
    'DataCleansing trim whitespace test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.trimmed AS expected_duration,
    actual.duration AS actual_duration
FROM expected_result expected
FULL OUTER JOIN actual_trimmed actual ON expected.id = actual.id
WHERE expected.trimmed IS DISTINCT FROM actual.duration
   OR expected.id IS NULL
   OR actual.id IS NULL

UNION ALL

SELECT
    'DataCleansing tabs, line breaks and duplicate whitespace test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.collapsed AS expected_duration,
    actual.duration AS actual_duration
FROM expected_result expected
FULL OUTER JOIN actual_collapsed actual ON expected.id = actual.id
WHERE expected.collapsed IS DISTINCT FROM actual.duration
   OR expected.id IS NULL
   OR actual.id IS NULL

UNION ALL

SELECT
    'DataCleansing all whitespace test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.no_whitespace AS expected_duration,
    actual.duration AS actual_duration
FROM expected_result expected
FULL OUTER JOIN actual_no_whitespace actual ON expected.id = actual.id
WHERE expected.no_whitespace IS DISTINCT FROM actual.duration
   OR expected.id IS NULL
   OR actual.id IS NULL
