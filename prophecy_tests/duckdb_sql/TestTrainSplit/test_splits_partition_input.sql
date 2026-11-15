-- Test: TestTrainSplit train/test/validation splits partition the input
-- Every input row must land in exactly one split, and no helper column may leak into the output.

WITH test_data AS (
    SELECT i AS id, 'name_' || CAST(i AS VARCHAR) AS name, i * 1.5 AS amount
    FROM range(1000) AS r(i)
),

train AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'train', 70, 20, 10, 42) }}
),

test AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'test', 70, 20, 10, 42) }}
),

validation AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'validation', 70, 20, 10, 42) }}
),

all_splits AS (
    SELECT * FROM train
    UNION ALL
    SELECT * FROM test
    UNION ALL
    SELECT * FROM validation
),

missing_rows AS (
    SELECT 'row missing from every split' AS failure, id FROM (
        SELECT * FROM test_data
        EXCEPT
        SELECT * FROM all_splits
    )
),

duplicated_rows AS (
    SELECT 'row present in more than one split' AS failure, id
    FROM all_splits
    GROUP BY id
    HAVING COUNT(*) > 1
)

-- Test passes if it returns 0 rows
SELECT * FROM missing_rows
UNION ALL
SELECT * FROM duplicated_rows
