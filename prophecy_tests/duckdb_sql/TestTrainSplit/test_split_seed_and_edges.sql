-- Test: TestTrainSplit is deterministic per seed, changes with the seed, and handles 0% / 100% splits

WITH test_data AS (
    SELECT i AS id
    FROM range(1000) AS r(i)
),

train_seed_1 AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'train', 50, 30, 20, 1) }}
),

train_seed_1_again AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'train', 50, 30, 20, 1) }}
),

train_seed_2 AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'train', 50, 30, 20, 2) }}
),

train_all AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'train', 100, 0, 0, 42) }}
),

test_none AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'test', 100, 0, 0, 42) }}
),

validation_none AS (
    {{ prophecy_basics.TestTrainSplit(['test_data'], 'validation', 100, 0, 0, 42) }}
),

failures AS (
    SELECT 'same seed gave a different split' AS failure
    WHERE (SELECT COUNT(*) FROM (SELECT * FROM train_seed_1 EXCEPT SELECT * FROM train_seed_1_again)) > 0
    UNION ALL
    SELECT 'different seeds gave the same split'
    WHERE (SELECT COUNT(*) FROM (SELECT * FROM train_seed_1 EXCEPT SELECT * FROM train_seed_2)) = 0
    UNION ALL
    SELECT '100% train did not keep every row'
    WHERE (SELECT COUNT(*) FROM train_all) != 1000
    UNION ALL
    SELECT '0% test / validation was not empty'
    WHERE (SELECT COUNT(*) FROM test_none) + (SELECT COUNT(*) FROM validation_none) != 0
)

-- Test passes if it returns 0 rows
SELECT * FROM failures
