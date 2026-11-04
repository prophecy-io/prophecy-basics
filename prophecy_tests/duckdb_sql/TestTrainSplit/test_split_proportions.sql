-- Test: TestTrainSplit split sizes follow the configured percentages
-- Hash bucketing is approximate, so each split must be within 5 percentage points of its target.

WITH test_data AS (
    SELECT i AS id, 'name_' || CAST(i AS VARCHAR) AS name
    FROM range(10000) AS r(i)
),

train AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'train', 60, 25, 15, 7) }}
),

test AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'test', 60, 25, 15, 7) }}
),

validation AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'validation', 60, 25, 15, 7) }}
),

split_sizes AS (
    SELECT 'train' AS split_name, 60 AS expected_pct, (SELECT COUNT(*) FROM train) * 100.0 / 10000 AS actual_pct
    UNION ALL
    SELECT 'test', 25, (SELECT COUNT(*) FROM test) * 100.0 / 10000
    UNION ALL
    SELECT 'validation', 15, (SELECT COUNT(*) FROM validation) * 100.0 / 10000
)

-- Test passes if it returns 0 rows
SELECT *
FROM split_sizes
WHERE ABS(actual_pct - expected_pct) > 5
