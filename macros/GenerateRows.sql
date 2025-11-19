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
    {% set unquoted_col = column_name | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# Use expressions directly and replace column_name with internal_col (matching applyPython logic) #}
    {% set init_select = init_expr | replace(column_name, internal_col) %}
    {% set condition_expr_sql = condition_expr | replace(column_name, internal_col) %}
    {% set loop_expr_replaced = loop_expr | replace(column_name, 'gen.' ~ internal_col) %}
    {% set recursion_condition = condition_expr_sql | replace(internal_col, 'gen.' ~ internal_col) %}
    {% set output_col_alias = column_name %}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {% if relation_tables %}
        with recursive gen as (
            -- base case: one row per input record
            select
                struct({{ alias }}.*) as payload,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from {{ relation_tables }} {{ alias }}

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
            -- Exclude column_name if it exists in payload to avoid duplicate column error
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

{% macro bigquery__GenerateRows(
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
        {% set init_select = "timestamp(" ~ init_value ~ ")" %}
    {% elif is_date %}
        {% set init_select = "date(" ~ init_value ~ ")" %}
    {% else %}
        {% set init_select = init_expr %}
    {% endif %}

    {# Normalize condition expression quotes #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# Replace column_name with internal_col in condition_expr #}
    {% set condition_expr_sql = condition_expr_sql | replace(column_name, internal_col) %}
    {# Replace payload. with nothing since columns are flattened in expanded CTE (used in final WHERE) #}
    {% set condition_expr_sql = condition_expr_sql | replace('payload.', '') %}

    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}
    {% set output_col_alias = prophecy_basics.quote_identifier(unquoted_col) %}
    {# In base case, remove table aliases since we are selecting from payload directly #}
    {% set init_select_base = init_select | replace('payload.', '') | replace(alias ~ '.', '') %}

    {% if relation_tables %}
        with payload as (
            -- Select all columns from source table
            select *
            from {{ relation_tables }} {{ alias }}
        ),
        base as (
            -- Base case: one row per input record with initial value
            -- Named 'base' so payload.column references work naturally
            select
                {% if column_name in ['a','b','c','d'] %}
                payload.* EXCEPT ({{ except_col }}),
                {% else %}
                payload.*,
                {% endif %}
                {{ init_select_base }} as {{ internal_col }}
            from payload
        ),
        sequences as (
            -- Generate sequence numbers
            select i as _iter
            from unnest(generate_array(1, {{ max_rows | int }})) as i
        ),
        expanded as (
            -- Generate array of values using ARRAY and UNNEST (similar to applyPython's transform/explode)
            -- Similar to applyPython: init + (i-1) * step
            -- Extract step: loop_expr(initial) - initial
            select
                base.* EXCEPT ({{ internal_col }}),
                gen._iter,
                gen.{{ internal_col }}
            from base
            cross join unnest(
                array(
                    select struct(
                        i as _iter,
                        base.{{ internal_col }} + (i - 1) * (({{ loop_expr | replace(column_name, internal_col) | replace('payload.', 'base.') }}) - base.{{ internal_col }}) as {{ internal_col }}
                    )
                    from unnest(generate_array(1, {{ max_rows | int }})) as i
                )
            ) as gen
        )
        select
            -- Select all original columns, then add the generated column
            expanded.* EXCEPT ({{ internal_col }}, _iter),
            {{ internal_col }} as {{ output_col_alias }}
        from expanded
        where _iter < {{ max_rows | int }}
          and ({{ condition_expr_sql }})
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

{% macro duckdb__GenerateRows(
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
    {% set unquoted_col = column_name | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# Use expressions directly and replace column_name with internal_col (matching applyPython logic) #}
    {% set init_select = init_expr | replace(column_name, internal_col) %}
    {% set condition_expr_sql = condition_expr | replace(column_name, internal_col) %}
    {% set loop_expr_replaced = loop_expr | replace(column_name, 'gen.' ~ internal_col) %}
    {% set recursion_condition = condition_expr_sql | replace(internal_col, 'gen.' ~ internal_col) %}
    {% set output_col_alias = column_name %}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {% if relation_tables %}
        with payload as (
            -- Select all columns from source table
            select *
            from {{ relation_tables }} {{ alias }}
        ),
        recursive gen as (
            -- base case: one row per input record
            select
                payload.*,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from payload

            union all

            -- recursive step
            select
                gen.* EXCLUDE ({{ internal_col }}, _iter),
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select
            -- Exclude column_name if it exists to avoid duplicate column error
            gen.* EXCLUDE ({{ except_col }}, _iter),
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