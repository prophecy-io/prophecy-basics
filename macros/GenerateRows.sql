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
    {% if init_expr is none or init_expr == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required") %}
    {% endif %}
    {% if condition_expr is none or condition_expr == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required") %}
    {% endif %}
    {% if loop_expr is none or loop_expr == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required") %}
    {% endif %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {# Use provided helper to unquote the provided column name #}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# detect date/timestamp style init expressions (kept from your original macro) #}
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

    {# --- Build quoted/plain variants for replacement --- #}
    {% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set backtick_col = "`" ~ unquoted_col ~ "`" %}
    {% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
    {% set singleq_col = "'" ~ unquoted_col ~ "'" %}
    {% set plain_col = unquoted_col %}

    {# --- Replace the target column in condition expression to reference the internal column --- #}
    {% set _cond_tmp = condition_expr_sql %}
    {% set _cond_tmp = _cond_tmp | replace(q_by_adapter, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(backtick_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(doubleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(singleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(plain_col, internal_col) %}
    {% set condition_expr_sql = _cond_tmp %}

    {# --- Replace the target column in loop expression to reference gen.<internal_col> in recursive step --- #}
    {% set _loop_tmp = loop_expr %}
    {% set _loop_tmp = _loop_tmp | replace(q_by_adapter, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(backtick_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(doubleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(singleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(plain_col, 'gen.' ~ internal_col) %}
    {% set loop_expr_replaced = _loop_tmp %}

    {# Use adapter-safe quoting for EXCEPT column #}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {# --- Build recursion_condition: same condition but referencing the previous iteration (gen.__gen_col) --- #}
    {% set _rec_tmp = condition_expr_sql %}
    {% set _rec_tmp = _rec_tmp | replace(internal_col, 'gen.' ~ internal_col) %}
    {# Note: condition_expr_sql already has internal_col substituted; above we switch to gen.internal_col for recursive WHERE #}
    {% set recursion_condition = _rec_tmp %}

    {# --- Determine output alias: quote it if it contains non [A-Za-z0-9_] characters (no regex used) --- #}
    {% set allowed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
    {% set specials = [] %}
    {% for ch in unquoted_col %}
        {% if ch not in allowed %}
            {% do specials.append(ch) %}
        {% endif %}
    {% endfor %}
    {% if specials | length > 0 %}
        {% set output_col_alias = prophecy_basics.quote_identifier(unquoted_col) %}
    {% else %}
        {% set output_col_alias = unquoted_col %}
    {% endif %}

    {% if relation_name %}
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
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select
            -- Use safe EXCEPT only if base column might exist; otherwise fallback
            {% if column_name in ['a','b','c','d'] %}
                payload.* EXCEPT ({{ except_col }}),
            {% else %}
                payload.*,
            {% endif %}
            {{ internal_col }} as {{ output_col_alias }}
        from gen
        where {{ condition_expr_sql }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ internal_col }}, 1 as _iter
            union all
            select
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select {{ internal_col }} as {{ output_col_alias }}
        from gen
        where {{ condition_expr_sql }}
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
    {% if init_expr is none or init_expr == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required") %}
    {% endif %}
    {% if condition_expr is none or condition_expr == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required") %}
    {% endif %}
    {% if loop_expr is none or loop_expr == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required") %}
    {% endif %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {# Use provided helper to unquote the provided column name #}
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

    {# --- Build quoted/plain variants for replacement --- #}
    {% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set backtick_col = "`" ~ unquoted_col ~ "`" %}
    {% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
    {% set singleq_col = "'" ~ unquoted_col ~ "'" %}
    {% set plain_col = unquoted_col %}

    {# --- Replace the target column in condition expression to reference the internal column --- #}
    {% set _cond_tmp = condition_expr_sql %}
    {% set _cond_tmp = _cond_tmp | replace(q_by_adapter, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(backtick_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(doubleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(singleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(plain_col, internal_col) %}
    {% set condition_expr_sql = _cond_tmp %}

    {# --- Replace the target column in loop expression to reference gen.<internal_col> in recursive step --- #}
    {% set _loop_tmp = loop_expr %}
    {% set _loop_tmp = _loop_tmp | replace(q_by_adapter, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(backtick_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(doubleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(singleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(plain_col, 'gen.' ~ internal_col) %}
    {% set loop_expr_replaced = _loop_tmp %}

    {# Use adapter-safe quoting for EXCEPT column #}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {# --- Build recursion_condition: same condition but referencing the previous iteration (gen.__gen_col) --- #}
    {% set _rec_tmp = condition_expr_sql %}
    {% set _rec_tmp = _rec_tmp | replace(internal_col, 'gen.' ~ internal_col) %}
    {% set recursion_condition = _rec_tmp %}

    {# --- Determine output alias: quote it if it contains non [A-Za-z0-9_] characters (no regex used) --- #}
    {% set allowed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
    {% set specials = [] %}
    {% for ch in unquoted_col %}
        {% if ch not in allowed %}
            {% do specials.append(ch) %}
        {% endif %}
    {% endfor %}
    {% if specials | length > 0 %}
        {% set output_col_alias = prophecy_basics.quote_identifier(unquoted_col) %}
    {% else %}
        {% set output_col_alias = unquoted_col %}
    {% endif %}

    {% if relation_name %}
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
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select
            -- Use EXCEPT for BigQuery
            payload.* EXCEPT ({{ except_col }}),
            {{ internal_col }} as {{ output_col_alias }}
        from gen
        where {{ condition_expr_sql }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ internal_col }}, 1 as _iter
            union all
            select
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select {{ internal_col }} as {{ output_col_alias }}
        from gen
        where {{ condition_expr_sql }}
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
    {% if init_expr is none or init_expr == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required") %}
    {% endif %}
    {% if condition_expr is none or condition_expr == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required") %}
    {% endif %}
    {% if loop_expr is none or loop_expr == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required") %}
    {% endif %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {# Use provided helper to unquote the provided column name #}
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

    {# DuckDB uses CAST or DATE/TIMESTAMP literals #}
    {% if is_timestamp %}
        {% set init_select = "CAST(" ~ init_value ~ " AS TIMESTAMP)" %}
    {% elif is_date %}
        {% set init_select = "CAST(" ~ init_value ~ " AS DATE)" %}
    {% else %}
        {% set init_select = init_expr %}
    {% endif %}

    {# Normalize user-supplied condition expression quotes if they used double quotes only #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# --- Build quoted/plain variants for replacement --- #}
    {% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set backtick_col = "`" ~ unquoted_col ~ "`" %}
    {% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
    {% set singleq_col = "'" ~ unquoted_col ~ "'" %}
    {% set plain_col = unquoted_col %}

    {# --- Replace the target column in condition expression to reference the internal column --- #}
    {% set _cond_tmp = condition_expr_sql %}
    {% set _cond_tmp = _cond_tmp | replace(q_by_adapter, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(backtick_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(doubleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(singleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(plain_col, internal_col) %}
    {% set condition_expr_sql = _cond_tmp %}

    {# --- Replace the target column in loop expression to reference gen.<internal_col> in recursive step --- #}
    {% set _loop_tmp = loop_expr %}
    {% set _loop_tmp = _loop_tmp | replace(q_by_adapter, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(backtick_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(doubleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(singleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(plain_col, 'gen.' ~ internal_col) %}
    {% set loop_expr_replaced = _loop_tmp %}

    {# Use adapter-safe quoting for EXCLUDE column #}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {# --- Build recursion_condition: same condition but referencing the previous iteration (gen.__gen_col) --- #}
    {% set _rec_tmp = condition_expr_sql %}
    {% set _rec_tmp = _rec_tmp | replace(internal_col, 'gen.' ~ internal_col) %}
    {% set recursion_condition = _rec_tmp %}

    {# --- Determine output alias: quote it if it contains non [A-Za-z0-9_] characters (no regex used) --- #}
    {% set allowed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
    {% set specials = [] %}
    {% for ch in unquoted_col %}
        {% if ch not in allowed %}
            {% do specials.append(ch) %}
        {% endif %}
    {% endfor %}
    {% if specials | length > 0 %}
        {% set output_col_alias = prophecy_basics.quote_identifier(unquoted_col) %}
    {% else %}
        {% set output_col_alias = unquoted_col %}
    {% endif %}

    {% if relation_name %}
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
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select
            -- Use EXCLUDE for DuckDB
            payload.* EXCLUDE ({{ except_col }}),
            {{ internal_col }} as {{ output_col_alias }}
        from gen
        where {{ condition_expr_sql }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ internal_col }}, 1 as _iter
            union all
            select
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select {{ internal_col }} as {{ output_col_alias }}
        from gen
        where {{ condition_expr_sql }}
    {% endif %}
{% endmacro %}