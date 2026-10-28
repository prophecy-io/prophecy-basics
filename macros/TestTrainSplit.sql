{#
  TestTrainSplit Macro Gem
  ========================

  Returns one split (train, test or validation) of the input relation. The gem
  emits one call per output port, so a model gets three CTEs built from the same
  input. Each row is assigned to a bucket in [0, 100) by a seeded hash over the
  whole row; because every call hashes the same way, the three splits never
  overlap and together contain every input row. Split sizes follow the
  percentages approximately (hash distribution), not exactly.

  Parameters:
    - relation_name (list): Relation identifier(s) to split (e.g. `['source_table']`).
    - split_name (string): Which split to return: 'train', 'test' or 'validation'.
    - train_pct (number): Percentage of rows in the train split.
    - test_pct (number): Percentage of rows in the test split.
    - validation_pct (number): Percentage of rows in the validation split. The
        validation split takes every bucket at or above train_pct + test_pct.
    - seed (int): Hash seed; change it to get a different (still deterministic) split.

  Bucket ranges:
    - train:      [0, train_pct)
    - test:       [train_pct, train_pct + test_pct)
    - validation: [train_pct + test_pct, 100)

  Adapter Support:
    - Default (Databricks / Spark): xxhash64 over the row, struct-packed so no helper column leaks
    - DuckDB: hash over the row struct
    - Snowflake: HASH(*) with SELECT * EXCLUDE
    - BigQuery: FARM_FINGERPRINT over TO_JSON_STRING(row) with SELECT * EXCEPT

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_basics.TestTrainSplit(['source_table'], 'train', 70, 20, 10, 42) }}
    {{ prophecy_basics.TestTrainSplit(['source_table'], 'test', 70, 20, 10, 42) }}
    {{ prophecy_basics.TestTrainSplit(['source_table'], 'validation', 70, 20, 10, 42) }}

  CTE Usage Example:
    Macro call (train example above), resolved query (default__):
      SELECT __split_row.* FROM (
        SELECT struct(*) AS __split_row, pmod(xxhash64(*, 42), 10000) / 100.0 AS __split_bucket
        FROM source_table
      ) WHERE __split_bucket >= 0 AND __split_bucket < 70
#}
{% macro TestTrainSplit(relation_name,
    split_name,
    train_pct,
    test_pct,
    validation_pct,
    seed) -%}
    {{ return(adapter.dispatch('TestTrainSplit', 'prophecy_basics')(relation_name,
    split_name,
    train_pct,
    test_pct,
    validation_pct,
    seed)) }}
{% endmacro %}

{#- Bucket filter on __split_bucket for the requested split. -#}
{%- macro test_train_split_condition(split_name, train_pct, test_pct) -%}
    {%- set train_upper = train_pct | float -%}
    {%- set test_upper = train_upper + (test_pct | float) -%}
    {%- if split_name == 'train' -%}
        __split_bucket >= 0 AND __split_bucket < {{ train_upper }}
    {%- elif split_name == 'test' -%}
        __split_bucket >= {{ train_upper }} AND __split_bucket < {{ test_upper }}
    {%- elif split_name == 'validation' -%}
        __split_bucket >= {{ test_upper }}
    {%- else -%}
        {{ exceptions.raise_compiler_error("TestTrainSplit: split_name must be 'train', 'test' or 'validation', got '" ~ split_name ~ "'") }}
    {%- endif -%}
{%- endmacro -%}

{%- macro default__TestTrainSplit(relation_name, split_name, train_pct, test_pct, validation_pct, seed) -%}
    {%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
    SELECT __split_row.*
    FROM (
        SELECT
            struct(*) AS __split_row,
            pmod(xxhash64(*, {{ seed | int }}), 10000) / 100.0 AS __split_bucket
        FROM {{ relation_list | join(', ') }}
    ) AS __split
    WHERE {{ prophecy_basics.test_train_split_condition(split_name, train_pct, test_pct) }}
{%- endmacro -%}

{%- macro duckdb__TestTrainSplit(relation_name, split_name, train_pct, test_pct, validation_pct, seed) -%}
    {%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
    SELECT __split_row.*
    FROM (
        SELECT
            __split_src AS __split_row,
            (hash(__split_src, {{ seed | int }}) % 10000) / 100.0 AS __split_bucket
        FROM {{ relation_list | join(', ') }} AS __split_src
    ) AS __split
    WHERE {{ prophecy_basics.test_train_split_condition(split_name, train_pct, test_pct) }}
{%- endmacro -%}

{%- macro snowflake__TestTrainSplit(relation_name, split_name, train_pct, test_pct, validation_pct, seed) -%}
    {%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
    SELECT * EXCLUDE (__split_bucket)
    FROM (
        SELECT
            *,
            ABS(MOD(HASH(HASH(*), {{ seed | int }}), 10000)) / 100.0 AS __split_bucket
        FROM {{ relation_list | join(', ') }}
    ) AS __split
    WHERE {{ prophecy_basics.test_train_split_condition(split_name, train_pct, test_pct) }}
{%- endmacro -%}

{%- macro bigquery__TestTrainSplit(relation_name, split_name, train_pct, test_pct, validation_pct, seed) -%}
    {%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
    SELECT * EXCEPT (__split_bucket)
    FROM (
        SELECT
            __split_src.*,
            ABS(MOD(FARM_FINGERPRINT(CONCAT(TO_JSON_STRING(__split_src), '|', '{{ seed | int }}')), 10000)) / 100.0 AS __split_bucket
        FROM `{{ relation_list | join('`, `') }}` AS __split_src
    ) AS __split
    WHERE {{ prophecy_basics.test_train_split_condition(split_name, train_pct, test_pct) }}
{%- endmacro -%}
