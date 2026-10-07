-- Test: CountRecords macro with total record count
-- This test validates that the CountRecords macro correctly counts rows

WITH test_data AS (
    -- Create sample test data
    SELECT 1 AS id, 'Alice' AS name, 100 AS amount
    UNION ALL
    SELECT 2 AS id, 'Bob' AS name, 200 AS amount
    UNION ALL
    SELECT 3 AS id, 'Charlie' AS name, NULL AS amount
    UNION ALL
    SELECT 4 AS id, NULL AS name, 400 AS amount
),

expected_result AS (
    -- Expected: 4 total records
    SELECT 4 AS expected_count
),

actual_result AS (
    -- Call the CountRecords macro to count total records
    {{ prophecy_basics.CountRecords('test_data', [], 'count_total_records') }}
)

-- Test passes if counts match (returns 0 rows)
-- Test fails if counts don't match (returns 1 row)
SELECT 
    'CountRecords total count test failed' AS test_name,
    expected.expected_count,
    actual.total_records AS actual_count
FROM expected_result expected
CROSS JOIN actual_result actual
WHERE expected.expected_count != actual.total_records

