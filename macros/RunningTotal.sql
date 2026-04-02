{#
  RunningTotal Macro Gem
  ======================

  Adds running totals for one or more numeric columns: each value shows the sum so
  far, either across the whole table or restarting within groups you specify, in
  the order you choose.

  Parameters:
    - relation_name (string or list): Source relation(s).
    - groupByColumnNames (list): PARTITION BY columns (empty = whole table).
    - runningTotalColumnNames (list): Columns to sum (coalesced to 0).
    - outputPrefix: Prefix for output column names.
    - orderByColumns (list[dict]): Window ORDER BY, same shape as Prophecy orderByColumns. Each item:
        { "expression": { "expression": "<SQL expression>" }, "sortType": "<sort>" }
        The inner expression is raw SQL (e.g. `col1`, concat(`a`,`b`)). sortType is one of:
        asc, desc, asc_nulls_last, desc_nulls_first (any other value is treated as desc).
        Empty list → default__ uses monotonically_increasing_id() in the window ORDER BY when no
        explicit order is built.

  Adapter Support:
    - default__, bigquery__ (frame only when ORDER BY present), snowflake__ (seq4() fallback), duckdb__

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {{ prophecy_basics.RunningTotal('t', ['region'], ['amt'], 'rt_', []) }}
    {% set orderByColumns = [
      {'expression': {'expression': '`order_date`'}, 'sortType': 'asc'},
      {'expression': {'expression': 'concat(`a`, `b`)'}, 'sortType': 'desc'}
    ] %}
    {{ prophecy_basics.RunningTotal('t', ['region'], ['amt'], 'rt_', orderByColumns) }}

  CTE Usage Example:
    Macro call (first example — empty orderByColumns; default__ falls back to monotonically_increasing_id()):
      {{ prophecy_basics.RunningTotal('t', ['region'], ['amt'], 'rt_', []) }}

    Resolved query (default__):
      with base as (
          select *
          from t
      )
      select
          base.*,
          sum(coalesce(base.`amt`, 0)) over (
              partition by `region`
              order by monotonically_increasing_id()
              rows between unbounded preceding and current row
          ) as `rt_amt`
      from base

    Macro call (second example — orderByColumns with expression + sortType as above):
      {% set orderByColumns = [
        {'expression': {'expression': '`order_date`'}, 'sortType': 'asc'},
        {'expression': {'expression': 'concat(`a`, `b`)'}, 'sortType': 'desc'}
      ] %}
      {{ prophecy_basics.RunningTotal('t', ['region'], ['amt'], 'rt_', orderByColumns) }}

    Resolved query (default__):
      with base as (
          select *
          from t
      )
      select
          base.*,
          sum(coalesce(base.`amt`, 0)) over (
              partition by `region`
              order by `order_date` asc, concat(`a`, `b`) desc
              rows between unbounded preceding and current row
          ) as `rt_amt`
      from base
#}
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

{%- if has_group -%}
    {%- if has_order -%}
        {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by " ~ order_by_clause ~ " rows between unbounded preceding and current row" -%}
    {%- else -%}
        {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by monotonically_increasing_id() rows between unbounded preceding and current row" -%}
    {%- endif -%}
{%- else -%}
    {%- if has_order -%}
        {%- set window_over = "order by " ~ order_by_clause ~ " rows between unbounded preceding and current row" -%}
    {%- else -%}
        {%- set window_over = "order by monotonically_increasing_id() rows between unbounded preceding and current row" -%}
    {%- endif -%}
{%- endif -%}

{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "sum(coalesce(base." ~ quoted_col ~ ", 0)) over (" ~ window_over ~ ") as " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

with base as (
    select *
    from {{ relation_list | join(', ') }}
)
select
    base.*,
    {{ run_tot_select_parts | join(',\n    ') }}
from base

{%- endmacro -%}


{%- macro bigquery__RunningTotal(
        relation_name,
        groupByColumnNames,
        runningTotalColumnNames,
        outputPrefix,
        orderByColumns= []
) -%}

{# ⚠️ BigQuery: no stable row id — results are non-deterministic without orderByColumns #}
{# ⚠️ BigQuery: ROWS frame clause only added when ORDER BY is present; BQ errors if frame is used without ORDER BY #}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set order_parts = [] -%}
{%- for r in orderByColumns %}
  {% if r.expression.expression | trim != '' %}
    {% set part %}
      {{ r.expression.expression }}
      {% if   r.sortType == 'asc'               %} ASC
      {% elif r.sortType == 'asc_nulls_last'    %} ASC NULLS LAST
      {% elif r.sortType == 'desc_nulls_first'  %} DESC NULLS FIRST
      {% else                               %} DESC
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

{%- if has_group -%}
    {%- if has_order -%}
        {%- set window_over = "PARTITION BY " ~ (quoted_group_columns | join(', ')) ~ " ORDER BY " ~ order_by_clause ~ " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" -%}
    {%- else -%}
        {%- set window_over = "PARTITION BY " ~ (quoted_group_columns | join(', ')) -%}
    {%- endif -%}
{%- else -%}
    {%- if has_order -%}
        {%- set window_over = "ORDER BY " ~ order_by_clause ~ " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" -%}
    {%- else -%}
        {%- set window_over = "" -%}
    {%- endif -%}
{%- endif -%}

{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "SUM(COALESCE(base." ~ quoted_col ~ ", 0)) OVER (" ~ window_over ~ ") AS " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

WITH base AS (
    SELECT *
    FROM {{ relation_list | join(', ') }}
)
SELECT
    base.*,
    {{ run_tot_select_parts | join(',\n    ') }}
FROM base

{%- endmacro -%}


{%- macro snowflake__RunningTotal(
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
      {% if   r.sortType == 'asc'               %} ASC
      {% elif r.sortType == 'asc_nulls_last'    %} ASC NULLS LAST
      {% elif r.sortType == 'desc_nulls_first'  %} DESC NULLS FIRST
      {% else                               %} DESC
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

{%- if has_group -%}
    {%- if has_order -%}
        {%- set window_over = "PARTITION BY " ~ (quoted_group_columns | join(', ')) ~ " ORDER BY " ~ order_by_clause ~ " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" -%}
    {%- else -%}
        {%- set window_over = "PARTITION BY " ~ (quoted_group_columns | join(', ')) ~ " ORDER BY seq4() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" -%}
    {%- endif -%}
{%- else -%}
    {%- if has_order -%}
        {%- set window_over = "ORDER BY " ~ order_by_clause ~ " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" -%}
    {%- else -%}
        {%- set window_over = "ORDER BY seq4() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" -%}
    {%- endif -%}
{%- endif -%}

{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "SUM(COALESCE(base." ~ quoted_col ~ ", 0)) OVER (" ~ window_over ~ ") AS " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

WITH base AS (
    SELECT *
    FROM {{ relation_list | join(', ') }}
)
SELECT
    base.*,
    {{ run_tot_select_parts | join(',\n    ') }}
FROM base

{%- endmacro -%}


{%- macro duckdb__RunningTotal(
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

{%- if has_group -%}
    {%- if has_order -%}
        {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by " ~ order_by_clause ~ " rows between unbounded preceding and current row" -%}
    {%- else -%}
        {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by (select 1) rows between unbounded preceding and current row" -%}
    {%- endif -%}
{%- else -%}
    {%- if has_order -%}
        {%- set window_over = "order by " ~ order_by_clause ~ " rows between unbounded preceding and current row" -%}
    {%- else -%}
        {%- set window_over = "order by (select 1) rows between unbounded preceding and current row" -%}
    {%- endif -%}
{%- endif -%}

{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "sum(coalesce(base." ~ quoted_col ~ ", 0)) over (" ~ window_over ~ ") as " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

with base as (
    select *
    from {{ relation_list | join(', ') }}
)
select
    base.*,
    {{ run_tot_select_parts | join(',\n    ') }}
from base

{%- endmacro -%}