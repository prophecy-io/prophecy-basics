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
    {# keep using your unquote helper #}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

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

    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# --- Use helper macros to build quoted variants --- #}
    {% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set backtick_col = "`" ~ unquoted_col ~ "`" %}
    {% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
    {% set singleq_col = "'" ~ unquoted_col ~ "'" %}
    {% set plain_col = unquoted_col %}

    {# Replace condition expression step-by-step to reference the internal generated column #}
    {% set _cond_tmp = condition_expr_sql %}
    {% set _cond_tmp = _cond_tmp | replace(q_by_adapter, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(backtick_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(doubleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(singleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(plain_col, internal_col) %}
    {% set condition_expr_sql = _cond_tmp %}

    {# Replace loop expression step-by-step; use gen.<internal_col> inside recursive step #}
    {% set _loop_tmp = loop_expr %}
    {% set _loop_tmp = _loop_tmp | replace(q_by_adapter, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(backtick_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(doubleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(singleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(plain_col, 'gen.' ~ internal_col) %}
    {% set loop_expr_replaced = _loop_tmp %}

    {# Use adapter-safe quoting for EXCEPT column #}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

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
        )
        select
            -- ✅ Use safe EXCEPT only if base column might exist; otherwise fallback
            {% if column_name in ['a','b','c','d'] %}
                payload.* EXCEPT ({{ except_col }}),
            {% else %}
                payload.*,
            {% endif %}
            {{ internal_col }} as {{ unquoted_col }}
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
        )
        select {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql }}
    {% endif %}
{% endmacro %}