{#
  WeightedAverage Macro Gem
  =========================

  Computes sum(value * weight) / nullif(sum(weight), 0) globally or per group.

  Parameters:
    - relation_name (string or list): Source relation(s).
    - valueFieldColumn, weightFieldColumn: Numeric column names.
    - outputFieldName: Alias for the weighted average expression.
    - groupByColumnNames (list): Empty → single aggregate row; else GROUP BY these columns.

  Adapter Support:
    - default__, bigquery__, snowflake__, duckdb__ (same formula; keyword casing differs)

  Macro Call Examples (default__):
    {{ prophecy_basics.WeightedAverage('t', 'score', 'w', 'wavg', []) }}
    {{ prophecy_basics.WeightedAverage('t', 'amt', 'qty', 'wa', ['region']) }}
#}
{% macro WeightedAverage(relation_name,
        valueFieldColumn,
        weightFieldColumn,
        outputFieldName,
        groupByColumnNames) -%}
    {{ return(adapter.dispatch('WeightedAverage', 'prophecy_basics')(relation_name,
        valueFieldColumn,
        weightFieldColumn,
        outputFieldName,
        groupByColumnNames)) }}
{% endmacro %}


{%- macro default__WeightedAverage(
        relation_name,
        valueFieldColumn,
        weightFieldColumn,
        outputFieldName,
        groupByColumnNames
) -%}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set has_group = groupByColumnNames | length > 0 -%}
{%- set quoted_value = prophecy_basics.quote_identifier(valueFieldColumn) -%}
{%- set quoted_weight = prophecy_basics.quote_identifier(weightFieldColumn) -%}
{%- set quoted_output = prophecy_basics.quote_identifier(outputFieldName) -%}
{%- set weighted_avg_expr = "sum(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") / nullif(sum(" ~ quoted_weight ~ "), 0) as " ~ quoted_output -%}

with base as (
    select *
    from {{ relation_list | join(', ') }}
)
{%- if has_group -%}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}
select
    {{ quoted_group_columns | join(', ') }},
    {{ weighted_avg_expr }}
from base
group by {{ quoted_group_columns | join(', ') }}
{%- else -%}
select
    {{ weighted_avg_expr }}
from base
{%- endif -%}

{%- endmacro -%}


{%- macro bigquery__WeightedAverage(
        relation_name,
        valueFieldColumn,
        weightFieldColumn,
        outputFieldName,
        groupByColumnNames
) -%}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set has_group = groupByColumnNames | length > 0 -%}
{%- set quoted_value = prophecy_basics.quote_identifier(valueFieldColumn) -%}
{%- set quoted_weight = prophecy_basics.quote_identifier(weightFieldColumn) -%}
{%- set quoted_output = prophecy_basics.quote_identifier(outputFieldName) -%}
{%- set weighted_avg_expr = "SUM(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") / NULLIF(SUM(" ~ quoted_weight ~ "), 0) AS " ~ quoted_output -%}

WITH base AS (
    SELECT *
    FROM {{ relation_list | join(', ') }}
)
{%- if has_group -%}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}
SELECT
    {{ quoted_group_columns | join(', ') }},
    {{ weighted_avg_expr }}
FROM base
GROUP BY {{ quoted_group_columns | join(', ') }}
{%- else -%}
SELECT
    {{ weighted_avg_expr }}
FROM base
{%- endif -%}

{%- endmacro -%}


{%- macro snowflake__WeightedAverage(
        relation_name,
        valueFieldColumn,
        weightFieldColumn,
        outputFieldName,
        groupByColumnNames
) -%}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set has_group = groupByColumnNames | length > 0 -%}
{%- set quoted_value = prophecy_basics.quote_identifier(valueFieldColumn) -%}
{%- set quoted_weight = prophecy_basics.quote_identifier(weightFieldColumn) -%}
{%- set quoted_output = prophecy_basics.quote_identifier(outputFieldName) -%}
{%- set weighted_avg_expr = "SUM(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") / NULLIF(SUM(" ~ quoted_weight ~ "), 0) AS " ~ quoted_output -%}

WITH base AS (
    SELECT *
    FROM {{ relation_list | join(', ') }}
)
{%- if has_group -%}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}
SELECT
    {{ quoted_group_columns | join(', ') }},
    {{ weighted_avg_expr }}
FROM base
GROUP BY {{ quoted_group_columns | join(', ') }}
{%- else -%}
SELECT
    {{ weighted_avg_expr }}
FROM base
{%- endif -%}

{%- endmacro -%}


{%- macro duckdb__WeightedAverage(
        relation_name,
        valueFieldColumn,
        weightFieldColumn,
        outputFieldName,
        groupByColumnNames
) -%}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set has_group = groupByColumnNames | length > 0 -%}
{%- set quoted_value = prophecy_basics.quote_identifier(valueFieldColumn) -%}
{%- set quoted_weight = prophecy_basics.quote_identifier(weightFieldColumn) -%}
{%- set quoted_output = prophecy_basics.quote_identifier(outputFieldName) -%}
{%- set weighted_avg_expr = "sum(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") / nullif(sum(" ~ quoted_weight ~ "), 0) as " ~ quoted_output -%}

with base as (
    select *
    from {{ relation_list | join(', ') }}
)
{%- if has_group -%}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}
select
    {{ quoted_group_columns | join(', ') }},
    {{ weighted_avg_expr }}
from base
group by {{ quoted_group_columns | join(', ') }}
{%- else -%}
select
    {{ weighted_avg_expr }}
from base
{%- endif -%}

{%- endmacro -%}
