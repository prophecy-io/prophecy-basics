{% macro RecordID(relation_name,
        method,
        record_id_column_name,
        incremental_id_type,
        incremental_id_size,
        incremental_id_starting_val,
        generationMethod,
        position,
        groupByColumnNames,
        orderByRules= []) -%}
    {{ return(adapter.dispatch('RecordID', 'prophecy_basics')(relation_name,
        method,
        record_id_column_name,
        incremental_id_type,
        incremental_id_size,
        incremental_id_starting_val,
        generationMethod,
        position,
        groupByColumnNames,
        orderByRules)) }}
{% endmacro %}


{%- macro default__RecordID(
        relation_name,
        method,
        record_id_column_name,
        incremental_id_type,
        incremental_id_size,
        incremental_id_starting_val,
        generationMethod,
        position,
        groupByColumnNames,
        orderByRules= []
) -%}

{# ── 1 · ORDER BY clause ──────────────────────────────────────────────────── #}
{%- set order_parts = [] -%}
{%- for r in orderByRules %}
  {% if r.expr | trim != '' %}
    {% set part %}
      {{ r.expr }}
      {% if   r.sort == 'asc'               %} asc
      {% elif r.sort == 'asc_nulls_last'    %} asc nulls last
      {% elif r.sort == 'desc_nulls_first'  %} desc nulls first
      {% else                               %} desc
      {% endif %}
    {% endset %}
    {%- do order_parts.append(part | trim) -%}
  {% endif %}
{%- endfor %}
{%- set order_by_clause = order_parts | join(', ') -%}
{%- set has_order = order_by_clause | length > 0 -%}
{%- set has_group = groupByColumnNames | length > 0 -%}

{# Quote column names for PARTITION BY clause #}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}

{# ── 2 · Record-ID expression ─────────────────────────────────────────────── #}
{%- if method == 'uuid' -%}
    {%- set id_expr = "uuid()" -%}
{%- else -%}
    {% set rn_expr %}
        row_number() over (
            {% if generationMethod == 'groupLevel' and has_group %}
                partition by {{ quoted_group_columns | join(', ') }}
            {% endif %}
            {% if generationMethod == 'groupLevel' and has_order %}
                order by {{ order_by_clause }}
            {% elif generationMethod == 'groupLevel' %}
                order by 1
            {% elif generationMethod == 'tableLevel' %}
                order by 1
            {% endif %}
        ) + {{ incremental_id_starting_val }} - 1
    {% endset %}

    {%- if incremental_id_type == 'string' -%}
        {%- set id_expr = "lpad(cast(" ~ (rn_expr | trim) ~ " as string), " ~ incremental_id_size ~ ", '0')" -%}
    {%- else -%}
        {%- set id_expr = rn_expr | trim -%}
    {%- endif -%}
{%- endif -%}

{# ── 3 · Final query ──────────────────────────────────────────────────────── #}
with base as (
    select *
    from {{ relation_name }}
),

enriched as (
    select
        {% if position == 'first_column' %}
            {{ id_expr }} as {{ record_id_column_name }},
            base.*
        {% else %}
            base.*,
            {{ id_expr }} as {{ record_id_column_name }}
        {% endif %}
    from base
)

select *
from enriched

{%- endmacro -%}

{%- macro bigquery__RecordID(
        relation_name,
        method,
        record_id_column_name,
        incremental_id_type,
        incremental_id_size,
        incremental_id_starting_val,
        generationMethod,
        position,
        groupByColumnNames,
        orderByRules= []
) -%}

{# ── 1 · ORDER BY clause ──────────────────────────────────────────────────── #}
{%- set order_parts = [] -%}
{%- for r in orderByRules %}
  {% if r.expr | trim != '' %}
    {% set part %}
      {{ r.expr }}
      {% if   r.sort == 'asc'               %} asc
      {% elif r.sort == 'asc_nulls_last'    %} asc nulls last
      {% elif r.sort == 'desc_nulls_first'  %} desc nulls first
      {% else                               %} desc
      {% endif %}
    {% endset %}
    {%- do order_parts.append(part | trim) -%}
  {% endif %}
{%- endfor %}
{%- set order_by_clause = order_parts | join(', ') -%}
{%- set has_order = order_by_clause | length > 0 -%}
{%- set has_group = groupByColumnNames | length > 0 -%}

{# Quote column names for PARTITION BY clause #}
{%- set quoted_group_columns = [] -%}
{%- for column in groupByColumnNames -%}
    {%- do quoted_group_columns.append(prophecy_basics.quote_identifier(column)) -%}
{%- endfor -%}

{# ── 2 · Record-ID expression ─────────────────────────────────────────────── #}
{%- if method == 'uuid' -%}
    {%- set id_expr = "GENERATE_UUID()" -%}
{%- else -%}
    {% set rn_expr %}
        row_number() over (
            {% if generationMethod == 'groupLevel' and has_group %}
                partition by {{ quoted_group_columns | join(', ') }}
            {% endif %}
            {% if generationMethod == 'groupLevel' and has_order %}
                order by {{ order_by_clause }}
            {% elif generationMethod == 'groupLevel' %}
                order by 1
            {% elif generationMethod == 'tableLevel' %}
                order by 1
            {% endif %}
        ) + {{ incremental_id_starting_val }} - 1
    {% endset %}

    {%- if incremental_id_type == 'string' -%}
        {%- set id_expr = "LPAD(CAST(" ~ (rn_expr | trim) ~ " AS STRING), " ~ incremental_id_size ~ ", '0')" -%}
    {%- else -%}
        {%- set id_expr = rn_expr | trim -%}
    {%- endif -%}
{%- endif -%}

{# ── 3 · Final query ──────────────────────────────────────────────────────── #}
with base as (
    select *
    from {{ relation_name }}
),

enriched as (
    select
        {% if position == 'first_column' %}
            {{ id_expr }} as {{ record_id_column_name }},
            base.*
        {% else %}
            base.*,
            {{ id_expr }} as {{ record_id_column_name }}
        {% endif %}
    from base
)

select *
from enriched

{%- endmacro -%}

{%- macro duckdb__RecordID(
        relation_name,
        method,
        record_id_column_name,
        incremental_id_type,
        incremental_id_size,
        incremental_id_starting_val,
        generationMethod,
        position,
        groupByColumnNames,
        orderByRules= []
) -%}

{# ── 1 · ORDER BY clause ──────────────────────────────────────────────────── #}
{%- set order_parts = [] -%}
{%- for r in orderByRules %}
  {% if r.expr | trim != '' %}
    {% set part %}
      {{ r.expr }}
      {% if   r.sort == 'asc'               %} asc
      {% elif r.sort == 'asc_nulls_last'    %} asc nulls last
      {% elif r.sort == 'desc_nulls_first'  %} desc nulls first
      {% else                               %} desc
      {% endif %}
    {% endset %}
    {%- do order_parts.append(part | trim) -%}
  {% endif %}
{%- endfor %}
{%- set order_by_clause = order_parts | join(', ') -%}
{%- set has_order = order_by_clause | length > 0 -%}
{%- set has_group = groupByColumnNames | length > 0 -%}

{# ── 2 · Record-ID expression ─────────────────────────────────────────────── #}
{%- if method == 'uuid' -%}
    {%- set id_expr = "uuid()" -%}
{%- else -%}
    {% set rn_expr %}
        row_number() over (
            {% if generationMethod == 'groupLevel' and has_group %}
                partition by {%- for col in groupByColumnNames -%}{{ prophecy_basics.quote_identifier(col) }}{% if not loop.last %}, {% endif %}{%- endfor -%}
            {% endif %}
            {% if generationMethod == 'groupLevel' and has_order %}
                order by {{ order_by_clause }}
            {% elif generationMethod == 'groupLevel' %}
                order by 1
            {% elif generationMethod == 'tableLevel' and has_order %}
                order by {{ order_by_clause }}
            {% elif generationMethod == 'tableLevel' %}
                order by 1
            {% endif %}
        ) + {{ incremental_id_starting_val }} - 1
    {% endset %}

    {%- if incremental_id_type == 'string' -%}
        {%- set id_expr = "lpad(cast(" ~ (rn_expr | trim) ~ " as varchar), " ~ incremental_id_size ~ ", '0')" -%}
    {%- else -%}
        {%- set id_expr = rn_expr | trim -%}
    {%- endif -%}
{%- endif -%}

{# ── 3 · Final query ──────────────────────────────────────────────────────── #}
with base as (
    select *
    from {{ relation_name }}
),

enriched as (
    select
        {% if position == 'first_column' %}
            {{ id_expr }} as {{ prophecy_basics.quote_identifier(record_id_column_name) }},
            base.*
        {% else %}
            base.*,
            {{ id_expr }} as {{ prophecy_basics.quote_identifier(record_id_column_name) }}
        {% endif %}
    from base
)

select *
from enriched

{%- endmacro -%}