{% macro RunningTotal(relation_name,
        groupByColumnNames,
        runningTotalColumnNames,
        outputPrefix) -%}
    {{ return(adapter.dispatch('RunningTotal', 'prophecy_basics')(relation_name,
        groupByColumnNames,
        runningTotalColumnNames,
        outputPrefix)) }}
{% endmacro %}


{%- macro default__RunningTotal(
        relation_name,
        groupByColumnNames,
        runningTotalColumnNames,
        outputPrefix
) -%}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set has_group = groupByColumnNames | length > 0 -%}

{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}

{%- if has_group -%}
    {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by 1" -%}
{%- else -%}
    {%- set window_over = "order by 1" -%}
{%- endif -%}

{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "sum(coalesce(" ~ quoted_col ~ ", 0)) over (" ~ window_over ~ ") as " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

select
    *,
    {{ run_tot_select_parts | join(',\n    ') }}
from {{ relation_list | join(', ') }}

{%- endmacro -%}
