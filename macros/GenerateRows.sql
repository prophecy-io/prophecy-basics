{% macro GenerateRows(relation_name,
    init_expr,
    condition_expr,
    loop_expr,
    column_name,
    max_rows,
    focus_mode) -%}
    {{ return(adapter.dispatch('GenerateRows', 'prophecy_basics')(relation_name,
    init_expr,
    condition_expr,
    loop_expr,
    column_name,
    max_rows,
    focus_mode)) }}
{% endmacro %}

{# ============================================ #}
{# DEFAULT (Spark/Databricks) Implementation   #}
{# ============================================ #}
{% macro default__GenerateRows(
    relation_name=None,
    init_expr='1',
    condition_expr='value <= 10',
    loop_expr='value + 1',
    column_name='value',
    max_rows=100000,
    focus_mode='recursive'
) %}
    {# Normalize empty strings to use defaults #}
    {% set init_expr = init_expr if init_expr and init_expr | trim != '' else '1' %}
    {% set condition_expr = condition_expr if condition_expr and condition_expr | trim != '' else 'value <= 10' %}
    {% set loop_expr = loop_expr if loop_expr and loop_expr | trim != '' else 'value + 1' %}
    
    {# Final validation #}
    {% if not init_expr or init_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required and cannot be empty") %}
    {% endif %}
    {% if not condition_expr or condition_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required and cannot be empty") %}
    {% endif %}
    {% if not loop_expr or loop_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required and cannot be empty") %}
    {% endif %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# detect date/timestamp style init expressions #}
    {% set is_timestamp = " " in init_expr %}
    {% set is_date = ("-" in init_expr) and not is_timestamp %}
    {% set init_strip = init_expr.strip() %}

    {% if init_strip.startswith("'") or init_strip.startswith('"') %}
        {% set init_value = init_strip %}
    {% else %}
        {% set init_value = "'" ~ init_strip ~ "'" %}
    {% endif %}

    {% if is_timestamp %}
        {% set init_select = "to_timestamp(" ~ init_value ~ ")" %}
    {% elif is_date %}
        {% set init_select = "to_date(" ~ init_value ~ ")" %}
    {% else %}
        {% set init_select = init_expr %}
    {% endif %}

    {# Normalize user-supplied condition expression quotes if they used double quotes only #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {% if relation_name and relation_name != '' and relation_name | trim != '' %}
        with recursive gen as (
            -- base case: one row per input record
            select
                struct({{ alias }}.*) as payload,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from {{ relation_name }} {{ alias }}

            union all

            -- recursive step
            select
                gen.payload as payload,
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ internal_col) }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select
            -- Use safe EXCEPT only if base column might exist; otherwise fallback
            {% if column_name in ['a','b','c','d'] %}
                payload.* EXCEPT ({{ unquoted_col }}),
            {% else %}
                payload.*,
            {% endif %}
            {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql | replace(unquoted_col, internal_col) }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ internal_col }}, 1 as _iter
            union all
            select
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ internal_col) }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql | replace(unquoted_col, internal_col) }}
    {% endif %}
{% endmacro %}

{# ============================================ #}
{# BIGQUERY Implementation                     #}
{# ============================================ #}
{% macro bigquery__GenerateRows(
    relation_name=None,
    init_expr='1',
    condition_expr='value <= 10',
    loop_expr='value + 1',
    column_name='value',
    max_rows=100000,
    focus_mode='recursive'
) %}
    {# Normalize empty strings to use defaults #}
    {% set init_expr = init_expr if init_expr and init_expr | trim != '' else '1' %}
    {% set condition_expr = condition_expr if condition_expr and condition_expr | trim != '' else 'value <= 10' %}
    {% set loop_expr = loop_expr if loop_expr and loop_expr | trim != '' else 'value + 1' %}
    
    {# Final validation #}
    {% if not init_expr or init_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required and cannot be empty") %}
    {% endif %}
    {% if not condition_expr or condition_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required and cannot be empty") %}
    {% endif %}
    {% if not loop_expr or loop_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required and cannot be empty") %}
    {% endif %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# detect date/timestamp style init expressions #}
    {% set is_timestamp = " " in init_expr %}
    {% set is_date = ("-" in init_expr) and not is_timestamp %}
    {% set init_strip = init_expr.strip() %}

    {% if init_strip.startswith("'") or init_strip.startswith('"') %}
        {% set init_value = init_strip %}
    {% else %}
        {% set init_value = "'" ~ init_strip ~ "'" %}
    {% endif %}

    {# BigQuery uses TIMESTAMP and DATE constructors or CAST #}
    {% if is_timestamp %}
        {% set init_select = "TIMESTAMP(" ~ init_value ~ ")" %}
    {% elif is_date %}
        {% set init_select = "DATE(" ~ init_value ~ ")" %}
    {% else %}
        {% set init_select = init_expr %}
    {% endif %}

    {# Normalize user-supplied condition expression quotes if they used double quotes only #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {% if relation_name and relation_name != '' and relation_name | trim != '' %}
        with recursive gen as (
            -- base case: one row per input record
            select
                STRUCT({{ alias }}.*) as payload,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from {{ relation_name }} {{ alias }}

            union all

            -- recursive step
            select
                gen.payload as payload,
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ internal_col) }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select
            -- Use EXCEPT for BigQuery
            payload.* EXCEPT ({{ unquoted_col }}),
            {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql | replace(unquoted_col, internal_col) }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ internal_col }}, 1 as _iter
            union all
            select
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ internal_col) }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql | replace(unquoted_col, internal_col) }}
    {% endif %}
{% endmacro %}

{# ============================================ #}
{# DUCKDB Implementation                       #}
{# ============================================ #}
{% macro duckdb__GenerateRows(
    relation_name=None,
    init_expr='1',
    condition_expr='value <= 10',
    loop_expr='value + 1',
    column_name='value',
    max_rows=100000,
    focus_mode='recursive'
) %}
    {# Normalize empty strings to use defaults #}
    {% set init_expr = init_expr if init_expr and init_expr | trim != '' else '1' %}
    {% set condition_expr = condition_expr if condition_expr and condition_expr | trim != '' else 'value <= 10' %}
    {% set loop_expr = loop_expr if loop_expr and loop_expr | trim != '' else 'value + 1' %}
    
    {# Final validation #}
    {% if not init_expr or init_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required and cannot be empty") %}
    {% endif %}
    {% if not condition_expr or condition_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required and cannot be empty") %}
    {% endif %}
    {% if not loop_expr or loop_expr | trim == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required and cannot be empty") %}
    {% endif %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# detect date/timestamp style init expressions #}
    {% set is_timestamp = " " in init_expr %}
    {% set is_date = ("-" in init_expr) and not is_timestamp %}
    {% set init_strip = init_expr.strip() %}

    {# If init_expr is a quoted string literal, extract the value #}
    {% if (init_strip.startswith("'") and init_strip.endswith("'")) or (init_strip.startswith('"') and init_strip.endswith('"')) %}
        {# Extract the inner value by removing quotes #}
        {% set inner_value = init_strip[1:-1] %}
        {# Check if it's a simple numeric value - try to detect numeric patterns #}
        {# Remove all digits, decimal points, and signs - if nothing remains, it's numeric #}
        {% set cleaned = inner_value | replace('0', '') | replace('1', '') | replace('2', '') | replace('3', '') | replace('4', '') | replace('5', '') | replace('6', '') | replace('7', '') | replace('8', '') | replace('9', '') | replace('.', '') | replace('-', '') | replace('+', '') | replace('e', '') | replace('E', '') | trim %}
        {% set is_numeric = inner_value | length > 0 and cleaned == '' and inner_value.count('.') <= 1 %}
        {% if is_numeric %}
            {# Use as numeric expression without quotes #}
            {% set init_select = inner_value %}
        {% elif is_timestamp %}
            {% set init_select = "CAST('" ~ inner_value ~ "' AS TIMESTAMP)" %}
        {% elif is_date %}
            {% set init_select = "CAST('" ~ inner_value ~ "' AS DATE)" %}
        {% else %}
            {# Use as string literal #}
            {% set init_select = "'" ~ inner_value ~ "'" %}
        {% endif %}
    {% else %}
        {# Not a quoted string, use as-is (could be an expression like 'value + 1') #}
        {% if is_timestamp %}
            {% set init_select = "CAST('" ~ init_strip ~ "' AS TIMESTAMP)" %}
        {% elif is_date %}
            {% set init_select = "CAST('" ~ init_strip ~ "' AS DATE)" %}
        {% else %}
            {% set init_select = init_expr %}
        {% endif %}
    {% endif %}

    {# Normalize user-supplied condition expression quotes if they used double quotes only #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {% if relation_name and relation_name != '' and relation_name | trim != '' %}
        with recursive gen as (
            -- base case: one row per input record
            -- Select all columns directly (no struct/ROW needed)
            select
                {{ alias }}.*,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from {{ relation_name }} {{ alias }}

            union all

            -- recursive step
            -- Select all columns from previous iteration, update generated column
            select
                gen.* EXCLUDE ({{ internal_col }}, _iter),
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ internal_col) }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select
            -- Select all columns except the internal counter and generated column, then add the final generated column
            gen.* EXCLUDE ({{ internal_col }}, _iter),
            {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql | replace(unquoted_col, internal_col) }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ internal_col }}, 1 as _iter
            union all
            select
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ internal_col) }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql | replace(unquoted_col, internal_col) }}
    {% endif %}
{% endmacro %}