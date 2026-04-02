{#
  CountRecords Macro Gem
  ======================

  Provides row counts for one or more tables and, when columns are specified,
  reports the number of non-null or distinct values in each. Useful for
  volume validation and quick data quality profiling.

  Parameters:
    - relation_name (list): Relation identifier(s) to count from (e.g. `['source_table']` or `[ref('my_model')]`).
    - column_names (list): List of column names to count. Used when count_method
        is "count_non_null_records" or "count_distinct_records". Pass an empty
        list [] when using the default total count.
    - count_method (string): Determines the counting strategy.
        - Any value other than the two below (e.g. "count_all_records"): Total row count via COUNT(*).
        - "count_non_null_records": Per-column non-null count via COUNT(col).
        - "count_distinct_records": Per-column distinct count via COUNT(DISTINCT col).

  Adapter Support:
    - Default (Databricks / Spark / Snowflake / DuckDB)
    - BigQuery (backtick-quoted table names)

  Depends on schema parameter:
    No

  Macro Call Examples:
    -- 1. Total record count
    {{ prophecy_basics.CountRecords(['source_table'], [], 'count_all_records') }}
    -- Generated SQL: SELECT COUNT(*) AS total_records FROM source_table

    -- 2. Non-null count per column
    {{ prophecy_basics.CountRecords(['source_table'], ['col_a', 'col_b'], 'count_non_null_records') }}
    -- Generated SQL: SELECT COUNT("col_a") AS "col_a_count", COUNT("col_b") AS "col_b_count" FROM source_table

    -- 3. Distinct count per column
    {{ prophecy_basics.CountRecords(['source_table'], ['col_a', 'col_b'], 'count_distinct_records') }}
    -- Generated SQL: SELECT COUNT(DISTINCT "col_a") AS "col_a_distinct_count", COUNT(DISTINCT "col_b") AS "col_b_distinct_count" FROM source_table

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.CountRecords(['source_table'], [], 'count_all_records') }}

    Resolved query (default__):
      SELECT COUNT(*) AS total_records FROM source_table
#}
{% macro CountRecords(relation_name,
    column_names,
    count_method) -%}
    {{ return(adapter.dispatch('CountRecords', 'prophecy_basics')(relation_name,
    column_names,
    count_method)) }}
{% endmacro %}

{%- macro default__CountRecords(relation_name,
    column_names,
    count_method
) %}
    
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {{ log("Computing record count from the table " ~ relation_name, info=True) }}
    {%- set select_query = "SELECT COUNT(*) AS total_records FROM " ~ (relation_list | join(', ')) -%}

    {%- if count_method == "count_non_null_records" -%}
        {{ log("Computing non null records count from the table for each column", info=True) }}
        {%- set withColumn_clause = [] -%}
        {% for column in column_names %}
            {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
            {%- do withColumn_clause.append("COUNT(" ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(column ~ "_count")) -%}
        {% endfor %}
        {%- set arg_string = withColumn_clause | join(', ') -%}
        {%- set select_query = "SELECT " ~ arg_string ~ " FROM " ~ (relation_list | join(', ')) -%}

    {%- elif count_method == "count_distinct_records" -%}
        {{ log("Computing distinct records count from the table for each column", info=True) }}
        {%- set withColumn_clause = [] -%}
        {% for column in column_names %}
            {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
            {%- do withColumn_clause.append("COUNT(DISTINCT " ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(column ~ "_distinct_count")) -%}
        {% endfor %}
        {%- set arg_string = withColumn_clause | join(', ') -%}
        {%- set select_query = "SELECT " ~ arg_string ~ " FROM " ~ (relation_list | join(', ')) -%}
    {%- endif -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(select_query, info=True) }}

    {{ return(select_query) }}
{%- endmacro -%}

{%- macro bigquery__CountRecords(relation_name,
    column_names,
    count_method
) %}
    
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {{ log("Computing record count from the table " ~ relation_name, info=True) }}
    {%- set select_query = "SELECT COUNT(*) AS total_records FROM `" ~ (relation_list | join('`, `')) ~ "`" -%}

    {%- if count_method == "count_non_null_records" -%}
        {{ log("Computing non null records count from the table for each column", info=True) }}
        {%- set withColumn_clause = [] -%}
        {% for column in column_names %}
            {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
            {%- do withColumn_clause.append("COUNT(" ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(column ~ "_count")) -%}
        {% endfor %}
        {%- set arg_string = withColumn_clause | join(', ') -%}
        {%- set select_query = "SELECT " ~ arg_string ~ " FROM " ~ (relation_list | join(', ')) -%}

    {%- elif count_method == "count_distinct_records" -%}
        {{ log("Computing distinct records count from the table for each column", info=True) }}
        {%- set withColumn_clause = [] -%}
        {% for column in column_names %}
            {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
            {%- do withColumn_clause.append("COUNT(DISTINCT " ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(column ~ "_distinct_count")) -%}
        {% endfor %}
        {%- set arg_string = withColumn_clause | join(', ') -%}
        {%- set select_query = "SELECT " ~ arg_string ~ " FROM " ~ (relation_list | join(', ')) -%}
    {%- endif -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(select_query, info=True) }}

    {{ return(select_query) }}
{%- endmacro -%}