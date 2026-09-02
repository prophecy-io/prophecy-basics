{#
  TestTrainSplit Macro Gem
  ========================

  Deterministically splits a table into "train" and "test" row subsets by hashing a chosen
  split column and comparing the hash against a train_percentage threshold. Both branches hash
  the SAME column with the SAME deterministic function and the SAME threshold, so they are
  mutually exclusive and exhaustive by construction: no row can satisfy both conditions, and
  every row satisfies exactly one - even though "train" and "test" compile to two fully
  independent queries with no shared state between them. Rows sharing the same split_column
  value always land in the same branch together (split granularity = distinct values, not
  individual rows).

  Parameters:
    - relation_name (list): Relation identifier(s) to split (e.g. `['source_table']`).
    - split_column (string): Column whose value is hashed to decide the branch.
    - train_percentage (int): Percentage of rows routed to "train" (0 < train_percentage < 100);
        the rest go to "test".
    - branch (string): Either 'train' or 'test' - selects which complementary half this call
        returns.

  Adapter Support:
    - Default (Databricks / Spark / Snowflake / DuckDB): HASH(split_column)
    - BigQuery: FARM_FINGERPRINT(CAST(split_column AS STRING))

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_basics.TestTrainSplit(['source_table'], 'customer_id', 80, 'train') }}
    {{ prophecy_basics.TestTrainSplit(['source_table'], 'customer_id', 80, 'test') }}
#}
{% macro TestTrainSplit(relation_name,
    split_column,
    train_percentage,
    branch) -%}
    {{ return(adapter.dispatch('TestTrainSplit', 'prophecy_basics')(relation_name,
    split_column,
    train_percentage,
    branch)) }}
{% endmacro %}

{%- macro default__TestTrainSplit(relation_name,
    split_column,
    train_percentage,
    branch
) %}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set quoted_column = prophecy_basics.quote_identifier(split_column) %}
    {% set comparison = "<" if branch == 'train' else ">=" %}
    {{ log("Splitting " ~ relation_name ~ " on " ~ split_column ~ " (" ~ branch ~ " branch, threshold " ~ train_percentage ~ ")", info=True) }}
    {%- set select_query = "SELECT * FROM " ~ (relation_list | join(', ')) ~
        " WHERE MOD(ABS(HASH(" ~ quoted_column ~ ")), 100) " ~ comparison ~ " " ~ train_percentage -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(select_query, info=True) }}

    {{ return(select_query) }}
{%- endmacro -%}

{%- macro bigquery__TestTrainSplit(relation_name,
    split_column,
    train_percentage,
    branch
) %}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set quoted_column = prophecy_basics.quote_identifier(split_column) %}
    {% set comparison = "<" if branch == 'train' else ">=" %}
    {{ log("Splitting " ~ relation_name ~ " on " ~ split_column ~ " (" ~ branch ~ " branch, threshold " ~ train_percentage ~ ")", info=True) }}
    {%- set select_query = "SELECT * FROM `" ~ (relation_list | join('`, `')) ~ "`" ~
        " WHERE MOD(ABS(FARM_FINGERPRINT(CAST(" ~ quoted_column ~ " AS STRING))), 100) " ~ comparison ~ " " ~ train_percentage -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(select_query, info=True) }}

    {{ return(select_query) }}
{%- endmacro -%}
