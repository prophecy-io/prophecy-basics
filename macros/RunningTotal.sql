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

{# row_id equivalent for deterministic ordering when no order or when order has duplicates #}
{%- set row_id_expr = "row_number() over (order by monotonically_increasing_id())" -%}

{%- if has_group -%}
    {%- if has_order -%}
        {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by " ~ order_by_clause ~ ", base._rn" -%}
    {%- else -%}
        {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by base._rn" -%}
    {%- endif -%}
{%- else -%}
    {%- if has_order -%}
        {%- set window_over = "order by " ~ order_by_clause ~ ", base._rn" -%}
    {%- else -%}
        {%- set window_over = "order by base._rn" -%}
    {%- endif -%}
{%- endif -%}

{# Cumulative sum: with ORDER BY, default frame is start-of-partition to current row in standard SQL and supported adapters #}
{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "sum(coalesce(base." ~ quoted_col ~ ", 0)) over (" ~ window_over ~ ") as " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

with base as (
    select *, {{ row_id_expr }} as _rn
    from {{ relation_list | join(', ') }}
)
select
    base.* except (_rn),
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

{# row_id equivalent for deterministic ordering when no order or when order has duplicates #}
{%- set row_id_expr = "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))" -%}

{%- if has_group -%}
    {%- if has_order -%}
        {%- set window_over = "PARTITION BY " ~ (quoted_group_columns | join(', ')) ~ " ORDER BY " ~ order_by_clause ~ ", base._rn" -%}
    {%- else -%}
        {%- set window_over = "PARTITION BY " ~ (quoted_group_columns | join(', ')) ~ " ORDER BY base._rn" -%}
    {%- endif -%}
{%- else -%}
    {%- if has_order -%}
        {%- set window_over = "ORDER BY " ~ order_by_clause ~ ", base._rn" -%}
    {%- else -%}
        {%- set window_over = "ORDER BY base._rn" -%}
    {%- endif -%}
{%- endif -%}

{# Cumulative sum: with ORDER BY, default frame is start-of-partition to current row in BigQuery #}
{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "SUM(COALESCE(base." ~ quoted_col ~ ", 0)) OVER (" ~ window_over ~ ") AS " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

WITH base AS (
    SELECT *, {{ row_id_expr }} AS _rn
    FROM {{ relation_list | join(', ') }}
)
SELECT
    base.* EXCEPT (_rn),
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

{# row_id equivalent for deterministic ordering when no order or when order has duplicates #}
{%- set row_id_expr = "ROW_NUMBER() OVER (ORDER BY (SELECT NULL))" -%}

{%- if has_group -%}
    {%- if has_order -%}
        {%- set window_over = "PARTITION BY " ~ (quoted_group_columns | join(', ')) ~ " ORDER BY " ~ order_by_clause ~ ", base._rn" -%}
    {%- else -%}
        {%- set window_over = "PARTITION BY " ~ (quoted_group_columns | join(', ')) ~ " ORDER BY base._rn" -%}
    {%- endif -%}
{%- else -%}
    {%- if has_order -%}
        {%- set window_over = "ORDER BY " ~ order_by_clause ~ ", base._rn" -%}
    {%- else -%}
        {%- set window_over = "ORDER BY base._rn" -%}
    {%- endif -%}
{%- endif -%}

{# Cumulative sum: with ORDER BY, default frame is start-of-partition to current row in Snowflake #}
{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "SUM(COALESCE(base." ~ quoted_col ~ ", 0)) OVER (" ~ window_over ~ ") AS " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

WITH base AS (
    SELECT *, {{ row_id_expr }} AS _rn
    FROM {{ relation_list | join(', ') }}
)
SELECT
    base.* EXCLUDE (_rn),
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

{# row_id equivalent for deterministic ordering when no order or when order has duplicates #}
{%- set row_id_expr = "row_number() over (order by (select null))" -%}

{%- if has_group -%}
    {%- if has_order -%}
        {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by " ~ order_by_clause ~ ", base._rn" -%}
    {%- else -%}
        {%- set window_over = "partition by " ~ (quoted_group_columns | join(', ')) ~ " order by base._rn" -%}
    {%- endif -%}
{%- else -%}
    {%- if has_order -%}
        {%- set window_over = "order by " ~ order_by_clause ~ ", base._rn" -%}
    {%- else -%}
        {%- set window_over = "order by base._rn" -%}
    {%- endif -%}
{%- endif -%}

{# Cumulative sum: with ORDER BY, default frame is start-of-partition to current row in DuckDB #}
{%- set run_tot_select_parts = [] -%}
{%- for column in runningTotalColumnNames -%}
    {%- set quoted_col = prophecy_basics.quote_identifier(column) -%}
    {%- set out_col = prophecy_basics.quote_identifier(outputPrefix ~ column) -%}
    {%- set expr = "sum(coalesce(base." ~ quoted_col ~ ", 0)) over (" ~ window_over ~ ") as " ~ out_col -%}
    {%- do run_tot_select_parts.append(expr) -%}
{%- endfor -%}

with base as (
    select *, {{ row_id_expr }} as _rn
    from {{ relation_list | join(', ') }}
)
select
    base.* exclude (_rn),
    {{ run_tot_select_parts | join(',\n    ') }}
from base

{%- endmacro -%}
