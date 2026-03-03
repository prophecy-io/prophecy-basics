{% macro RunningTotal(relation_name,
        groupByColumnNames,
        runningTotalColumnNames,
        outputPrefix,
        orderByColumns= []) -%}
    {{ return(adapter.dispatch('RunningTotal', 'prophecy_basics')(relation_name,
        groupByColumnNames,
        runningTotalColumnNames,
        outputPrefix,
        orderByColumns)) }}
{% endmacro %}


{%- macro default__RunningTotal(
        relation_name,
        groupByColumnNames,
        runningTotalColumnNames,
        outputPrefix,
        orderByColumns= []
) -%}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set order_parts = [] -%}
{%- for r in orderByColumns %}
  {% if r.expression.expression | trim != '' %}
    {% set part %}
      {{ r.expression.expression }}
      {% if   r.sortType == 'asc'               %} asc
      {% elif r.sortType == 'asc_nulls_last'    %} asc nulls last
      {% elif r.sortType == 'desc_nulls_first'  %} desc nulls first
      {% else                               %} desc
      {% endif %}
    {% endset %}
    {%- do order_parts.append(part | trim) -%}
  {% endif %}
{%- endfor %}
{%- set order_by_clause = order_parts | join(', ') -%}
{%- set has_order = order_by_clause | length > 0 -%}
{%- set has_group = groupByColumnNames | length > 0 -%}

{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}

{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set window_expr %}
        sum({{ quoted_col }}) over (
            {% if has_group %}partition by {{ quoted_group_columns | join(', ') }} {% endif %}
            {% if has_order %}order by {{ order_by_clause }}{% else %}order by 1{% endif %}
        )
    {% endset %}
    {%- do run_tot_select_parts.append((window_expr | trim) ~ " as " ~ out_col) -%}
{%- endfor -%}

select
    *,
    {{ run_tot_select_parts | join(',\n    ') }}
from {{ relation_list | join(', ') }}

{%- endmacro -%}
