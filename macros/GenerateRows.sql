{% macro GenerateRows(relation_name,
    init_expr,
    condition_expr,
    loop_expr,
    column_name,
    max_rows,
    force_mode) -%}
    {{ return(adapter.dispatch('GenerateRows', 'prophecy_basics')(relation_name,
    init_expr,
    condition_expr,
    loop_expr,
    column_name,
    max_rows,
    force_mode)) }}
{% endmacro %}

{% macro default__GenerateRows(
    relation_name=None,
    init_expr='1',
    condition_expr='value <= 10',
    loop_expr='value + 1',
    column_name='value',
    max_rows=100000,
    force_mode='recursive'
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
    {% set relation_tables = (relation_name if relation_name is iterable and relation_name is not string else [relation_name]) | join(', ')  %}
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

    {% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set backtick_col = "`" ~ unquoted_col ~ "`" %}
    {% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
    {% set singleq_col = "'" ~ unquoted_col ~ "'" %}
    {% set plain_col = unquoted_col %}

    {% set _cond_tmp = condition_expr_sql %}
    {% set _cond_tmp = _cond_tmp | replace(q_by_adapter, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(backtick_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(doubleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(singleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(plain_col, internal_col) %}
    {% set condition_expr_sql = _cond_tmp %}
    {% set condition_expr_sql = condition_expr_sql | replace('payload.gen.', 'gen.') %}

    {% set _loop_tmp = loop_expr %}
    {% set _loop_tmp = _loop_tmp | replace('payload.' ~ q_by_adapter, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace('payload.' ~ backtick_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace('payload.' ~ doubleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace('payload.' ~ singleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace('payload.' ~ plain_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(q_by_adapter, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(backtick_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(doubleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(singleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(plain_col, 'gen.' ~ internal_col) %}
    {% set loop_expr_replaced = _loop_tmp %}
    {% set loop_expr_replaced = loop_expr_replaced | replace('payload.gen.', 'gen.') %}

    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {% set _rec_tmp = condition_expr_sql %}
    {% set _rec_tmp = _rec_tmp | replace(internal_col, 'gen.' ~ internal_col) %}
    {% set recursion_condition = _rec_tmp %}
    {% set recursion_condition = recursion_condition | replace('payload.gen.', 'gen.') %}

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

    {% if relation_tables %}
        with recursive gen as (
            select
                struct({{ alias }}.*) as payload,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from {{ relation_tables }} {{ alias }}

            union all

            select
                gen.payload as payload,
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select
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