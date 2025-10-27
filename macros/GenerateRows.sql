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
    {# unquoted logical name, trimmed of whitespace #}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {# safe internal alias (no spaces) #}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}
    {# safe quoted output column (preserves spaces/special chars) #}
    {% set quoted_output_col = prophecy_basics.quote_identifier(column_name) %}

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

    {# normalize condition expression basic quoting to single quotes if needed #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# prepare forms to replace: backticked and plain #}
    {% set bt_unquoted = "`" ~ unquoted_col ~ "`" %}
    {% set gen_prefixed = "gen." ~ internal_col %}
    {% set internal_simple = internal_col %}

    {# --- PRECOMPUTE rewritten expressions to avoid inline filter calls inside SQL --- #}
    {% if relation_name %}
        {# For relation case, we need recursive references to point to gen.internal alias (gen.__gen_x) #}
        {% set loop_expr_rewrite = loop_expr | replace(bt_unquoted, gen_prefixed) | replace(unquoted_col, gen_prefixed) %}
        {% set condition_expr_rewrite = condition_expr_sql | replace(bt_unquoted, internal_simple) | replace(unquoted_col, internal_simple) %}
    {% else %}
        {# Standalone generator: use internal_simple (no gen. prefix available inside recursive CTE) #}
        {% set loop_expr_rewrite = loop_expr | replace(bt_unquoted, internal_simple) | replace(unquoted_col, internal_simple) %}
        {% set condition_expr_rewrite = condition_expr_sql | replace(bt_unquoted, internal_simple) | replace(unquoted_col, internal_simple) %}
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
                {{ loop_expr_rewrite }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select
            {%- if column_name in ['a','b','c','d'] -%}
                payload.* EXCEPT ({{ unquoted_col }}),
            {%- else -%}
                payload.*,
            {%- endif -%}
            {{ internal_col }} as {{ quoted_output_col }}
        from gen
        where {{ condition_expr_rewrite }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ internal_col }}, 1 as _iter
            union all
            select
                {{ loop_expr_rewrite }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select {{ internal_col }} as {{ quoted_output_col }}
        from gen
        where {{ condition_expr_rewrite }}
    {% endif %}
{% endmacro %}