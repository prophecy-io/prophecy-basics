-- Test: CountRecords macro with non-null record count
-- This test validates that the CountRecords macro correctly counts non-null values per column

WITH test_data AS (
    -- Create sample test data with nulls
    SELECT 1 AS id, 'Alice' AS name, 100 AS amount
    UNION ALL
    SELECT 2 AS id, 'Bob' AS name, 200 AS amount
    UNION ALL
    SELECT 3 AS id, 'Charlie' AS name, NULL AS amount  -- NULL amount
    UNION ALL
    SELECT 4 AS id, NULL AS name, 400 AS amount  -- NULL name
),

expected_result AS (
    -- Expected: id has 4 non-null, name has 3 non-null, amount has 3 non-null
    SELECT 
        4 AS expected_id_count,
        3 AS expected_name_count,
        3 AS expected_amount_count
),

actual_result AS (
    -- Call the CountRecords macro to count non-null records per column
    {{ prophecy_basics.CountRecords('test_data', ['id', 'name', 'amount'], 'count_non_null_records') }}
)

-- Test passes if all counts match (returns 0 rows)
-- Test fails if any count doesn't match (returns rows)
SELECT 
    'CountRecords non-null test failed' AS test_name,
    'id' AS column_name,
    expected.expected_id_count AS expected_count,
    actual.id_count AS actual_count
FROM expected_result expected
CROSS JOIN actual_result actual
WHERE expected.expected_id_count != actual.id_count

UNION ALL

SELECT 
    'CountRecords non-null test failed' AS test_name,
    'name' AS column_name,
    expected.expected_name_count AS expected_count,
    actual.name_count AS actual_count
FROM expected_result expected
CROSS JOIN actual_result actual
WHERE expected.expected_name_count != actual.name_count

UNION ALL

SELECT 
    'CountRecords non-null test failed' AS test_name,
    'amount' AS column_name,
    expected.expected_amount_count AS expected_count,
    actual.amount_count AS actual_count
FROM expected_result expected
CROSS JOIN actual_result actual
WHERE expected.expected_amount_count != actual.amount_count

