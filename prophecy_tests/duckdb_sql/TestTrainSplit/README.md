# TestTrainSplit Macro Tests

This directory contains dbt tests for the `TestTrainSplit` macro (`macros/TestTrainSplit.sql`).

## Test Files

1. **`test_splits_partition_input.sql`**: the three splits are disjoint, cover every input row, and keep the input columns (no helper column leaks).
2. **`test_split_proportions.sql`**: each split is within 5 percentage points of its configured percentage.
3. **`test_split_seed_and_edges.sql`**: the same seed gives the same split, a different seed gives a different one, and 100/0/0 percentages work.

**Tests pass when they return 0 rows.**
