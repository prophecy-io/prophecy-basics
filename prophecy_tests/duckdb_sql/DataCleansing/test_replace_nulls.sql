-- Test: DataCleansing macro with Replace null values in column
-- This test validates the string, numeric, Date and Time options together: every column is
-- selected and every option is on, so each NULL must get the replacement for its own type
-- and non-NULL values must stay unchanged.
-- The Time value uses the gem's default format (with ".0").

WITH test_data AS (
    -- Create sample test data: row 1 has values, row 2 has NULL in every cleaned column
    SELECT 1 AS id, 'Alice' AS name, 5 AS qty, DATE '2024-01-15' AS start_date, TIMESTAMP '2024-01-15 10:30:00' AS updated_at
    UNION ALL
    SELECT 2 AS id, NULL AS name, NULL AS qty, NULL AS start_date, NULL AS updated_at
),

expected_result AS (
    -- Expected: row 1 unchanged; row 2 NULLs replaced by the value for each type
    SELECT 1 AS id, 'Alice' AS name, 5 AS qty, DATE '2024-01-15' AS start_date, TIMESTAMP '2024-01-15 10:30:00' AS updated_at
    UNION ALL
    SELECT 2 AS id, 'Unknown' AS name, -1 AS qty, DATE '1970-01-01' AS start_date, TIMESTAMP '1970-01-01 00:00:00' AS updated_at
),

actual_result AS (
    -- Call the DataCleansing macro with every null replacement option on
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "name", "dataType": "String"}, {"name": "qty", "dataType": "Integer"}, {"name": "start_date", "dataType": "Date"}, {"name": "updated_at", "dataType": "Timestamp"}],
        modifyCase='keepOriginal',
        columnNames=['name', 'qty', 'start_date', 'updated_at'],
        replaceNullTextFields=true,
        replaceNullTextWith='Unknown',
        replaceNullForNumericFields=true,
        replaceNullNumericWith=-1,
        replaceNullDateFields=true,
        replaceNullDateWith='1970-01-01',
        replaceNullTimeFields=true,
        replaceNullTimeWith='1970-01-01 00:00:00.0'
    ) }}
)

-- Test passes if every row matches (returns 0 rows)
-- Test fails if any value differs or a row is missing (returns rows)
SELECT
    'DataCleansing replace nulls test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id
FROM expected_result expected
FULL OUTER JOIN actual_result actual ON expected.id = actual.id
WHERE expected.name IS DISTINCT FROM actual.name
   OR expected.qty IS DISTINCT FROM actual.qty
   OR expected.start_date IS DISTINCT FROM actual.start_date
   OR expected.updated_at IS DISTINCT FROM actual.updated_at
   OR expected.id IS NULL
   OR actual.id IS NULL
