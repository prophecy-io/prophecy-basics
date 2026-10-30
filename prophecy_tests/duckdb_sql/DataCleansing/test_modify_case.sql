-- Test: DataCleansing macro with Modify case
-- This test validates lowercase, UPPERCASE and Title Case, and that NULL stays NULL.
-- Title Case input has single spaces and no punctuation, where all engines agree.

WITH test_data AS (
    -- Create sample test data with mixed case
    SELECT 1 AS id, 'hello wORLD' AS name
    UNION ALL
    SELECT 2 AS id, 'ABC def' AS name
    UNION ALL
    SELECT 3 AS id, NULL AS name  -- NULL value
),

expected_result AS (
    -- Expected result for each case option
    SELECT 1 AS id, 'hello world' AS lower_name, 'HELLO WORLD' AS upper_name, 'Hello World' AS title_name
    UNION ALL
    SELECT 2 AS id, 'abc def' AS lower_name, 'ABC DEF' AS upper_name, 'Abc Def' AS title_name
    UNION ALL
    SELECT 3 AS id, NULL AS lower_name, NULL AS upper_name, NULL AS title_name
),

actual_lower AS (
    -- lowercase
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "name", "dataType": "String"}],
        modifyCase='makeLowercase',
        columnNames=['name']
    ) }}
),

actual_upper AS (
    -- UPPERCASE
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "name", "dataType": "String"}],
        modifyCase='makeUppercase',
        columnNames=['name']
    ) }}
),

actual_title AS (
    -- Title Case
    {{ prophecy_basics.DataCleansing(
        relation_name=['test_data'],
        schema=[{"name": "id", "dataType": "Integer"}, {"name": "name", "dataType": "String"}],
        modifyCase='makeTitlecase',
        columnNames=['name']
    ) }}
)

-- Test passes if every row matches (returns 0 rows)
-- Test fails if any value differs or a row is missing (returns rows)
SELECT
    'DataCleansing lowercase test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.lower_name AS expected_name,
    actual.name AS actual_name
FROM expected_result expected
FULL OUTER JOIN actual_lower actual ON expected.id = actual.id
WHERE expected.lower_name IS DISTINCT FROM actual.name
   OR expected.id IS NULL
   OR actual.id IS NULL

UNION ALL

SELECT
    'DataCleansing UPPERCASE test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.upper_name AS expected_name,
    actual.name AS actual_name
FROM expected_result expected
FULL OUTER JOIN actual_upper actual ON expected.id = actual.id
WHERE expected.upper_name IS DISTINCT FROM actual.name
   OR expected.id IS NULL
   OR actual.id IS NULL

UNION ALL

SELECT
    'DataCleansing Title Case test failed' AS test_name,
    COALESCE(expected.id, actual.id) AS id,
    expected.title_name AS expected_name,
    actual.name AS actual_name
FROM expected_result expected
FULL OUTER JOIN actual_title actual ON expected.id = actual.id
WHERE expected.title_name IS DISTINCT FROM actual.name
   OR expected.id IS NULL
   OR actual.id IS NULL
