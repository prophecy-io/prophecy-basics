-- Test: CountRecords macro with distinct record count
-- This test validates that the CountRecords macro correctly counts distinct values per column

WITH test_data AS (
    -- Create sample test data with duplicates
    SELECT 1 AS id, 'Alice' AS name, 'Gold' AS tier
    UNION ALL
    SELECT 2 AS id, 'Bob' AS name, 'Silver' AS tier
    UNION ALL
    SELECT 3 AS id, 'Charlie' AS name, 'Gold' AS tier  -- Duplicate tier
    UNION ALL
    SELECT 4 AS id, 'Diana' AS name, 'Gold' AS tier  -- Duplicate tier
    UNION ALL
    SELECT 5 AS id, 'Bob' AS name, 'Bronze' AS tier  -- Duplicate name
),

expected_result AS (
    -- Expected: id has 5 distinct, name has 4 distinct (Bob appears twice), tier has 3 distinct
    SELECT 
        5 AS expected_id_distinct,
        4 AS expected_name_distinct,
        3 AS expected_tier_distinct
),

actual_result AS (
    -- Call the CountRecords macro to count distinct records per column
    {{ prophecy_basics.CountRecords('test_data', ['id', 'name', 'tier'], 'count_distinct_records') }}
)

-- Test passes if all counts match (returns 0 rows)
-- Test fails if any count doesn't match (returns rows)
SELECT 
    'CountRecords distinct test failed' AS test_name,
    'id' AS column_name,
    expected.expected_id_distinct AS expected_count,
    actual.id_distinct_count AS actual_count
FROM expected_result expected
CROSS JOIN actual_result actual
WHERE expected.expected_id_distinct != actual.id_distinct_count

UNION ALL

SELECT 
    'CountRecords distinct test failed' AS test_name,
    'name' AS column_name,
    expected.expected_name_distinct AS expected_count,
    actual.name_distinct_count AS actual_count
FROM expected_result expected
CROSS JOIN actual_result actual
WHERE expected.expected_name_distinct != actual.name_distinct_count

UNION ALL

SELECT 
    'CountRecords distinct test failed' AS test_name,
    'tier' AS column_name,
    expected.expected_tier_distinct AS expected_count,
    actual.tier_distinct_count AS actual_count
FROM expected_result expected
CROSS JOIN actual_result actual
WHERE expected.expected_tier_distinct != actual.tier_distinct_count

