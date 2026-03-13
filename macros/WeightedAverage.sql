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
{%- if has_group -%}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}
{%- set partition_clause = "partition by " ~ (quoted_group_columns | join(', ')) -%}
{%- set weighted_avg_expr = "sum(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") over (" ~ partition_clause ~ ") / nullif(sum(" ~ quoted_weight ~ ") over (" ~ partition_clause ~ "), 0) as " ~ quoted_output -%}
{%- else -%}
{%- set weighted_avg_expr = "sum(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") over () / nullif(sum(" ~ quoted_weight ~ ") over (), 0) as " ~ quoted_output -%}
{%- endif -%}

with base as (
    select *
    from {{ relation_list | join(', ') }}
)
select *,
    {{ weighted_avg_expr }}
from base

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
{%- if has_group -%}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}
{%- set partition_clause = "PARTITION BY " ~ (quoted_group_columns | join(', ')) -%}
{%- set weighted_avg_expr = "SUM(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") OVER (" ~ partition_clause ~ ") / NULLIF(SUM(" ~ quoted_weight ~ ") OVER (" ~ partition_clause ~ "), 0) AS " ~ quoted_output -%}
{%- else -%}
{%- set weighted_avg_expr = "SUM(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") OVER () / NULLIF(SUM(" ~ quoted_weight ~ ") OVER (), 0) AS " ~ quoted_output -%}
{%- endif -%}

WITH base AS (
    SELECT *
    FROM {{ relation_list | join(', ') }}
)
SELECT *,
    {{ weighted_avg_expr }}
FROM base

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
{%- if has_group -%}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}
{%- set partition_clause = "PARTITION BY " ~ (quoted_group_columns | join(', ')) -%}
{%- set weighted_avg_expr = "SUM(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") OVER (" ~ partition_clause ~ ") / NULLIF(SUM(" ~ quoted_weight ~ ") OVER (" ~ partition_clause ~ "), 0) AS " ~ quoted_output -%}
{%- else -%}
{%- set weighted_avg_expr = "SUM(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") OVER () / NULLIF(SUM(" ~ quoted_weight ~ ") OVER (), 0) AS " ~ quoted_output -%}
{%- endif -%}

WITH base AS (
    SELECT *
    FROM {{ relation_list | join(', ') }}
)
SELECT *,
    {{ weighted_avg_expr }}
FROM base

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
{%- if has_group -%}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}
{%- set partition_clause = "partition by " ~ (quoted_group_columns | join(', ')) -%}
{%- set weighted_avg_expr = "sum(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") over (" ~ partition_clause ~ ") / nullif(sum(" ~ quoted_weight ~ ") over (" ~ partition_clause ~ "), 0) as " ~ quoted_output -%}
{%- else -%}
{%- set weighted_avg_expr = "sum(" ~ quoted_value ~ " * " ~ quoted_weight ~ ") over () / nullif(sum(" ~ quoted_weight ~ ") over (), 0) as " ~ quoted_output -%}
{%- endif -%}

with base as (
    select *
    from {{ relation_list | join(', ') }}
)
select *,
    {{ weighted_avg_expr }}
from base

{%- endmacro -%}
