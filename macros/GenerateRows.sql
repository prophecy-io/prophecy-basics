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
    {% set unquoted_col = column_name | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# Use expressions directly and replace column_name with internal_col (matching applyPython logic) #}
    {% set init_select = init_expr | replace(column_name, internal_col) %}
    {% set condition_expr_sql = condition_expr | replace(column_name, internal_col) %}
    {% set loop_expr_replaced = loop_expr | replace(column_name, internal_col) %}
    {% set output_col_alias = column_name %}
    {# BigQuery EXCEPT requires unquoted identifiers, not quoted strings #}
    {% set except_col = unquoted_col %}
    {# In base case, remove table aliases since we're selecting from payload directly #}
    {% set init_select_base = init_select | replace('payload.', '') | replace(alias ~ '.', '') %}

    {% if relation_tables %}
        with payload as (
            -- Select all columns from source table
            select *
            from {{ relation_tables }} {{ alias }}
        ),
        base as (
            -- Base case: one row per input record with initial value
            select
                payload.* EXCEPT ({{ except_col }}),
                {{ init_select_base }} as {{ internal_col }}
            from payload
        ),
        sequences as (
            -- Generate sequence numbers
            select i as _iter
            from unnest(generate_array(1, {{ max_rows | int }})) as i
        ),
        expanded as (
            -- Cross join base with sequences and calculate values iteratively
            -- For linear increments: value = init + (iter - 1) * step
            -- Step = loop_expr(init) - init
            select
                base.* EXCEPT ({{ internal_col }}),
                sequences._iter,
                -- Calculate value for this iteration: init + (iter-1) * (loop_expr(init) - init)
                base.{{ internal_col }} + (sequences._iter - 1) * (({{ loop_expr_replaced | replace(internal_col, 'base.' ~ internal_col) }}) - base.{{ internal_col }}) as {{ internal_col }}
            from base
            cross join sequences
        )
        select
            -- Select all original columns, then add the generated column
            expanded.* EXCEPT ({{ internal_col }}, _iter),
            {{ internal_col }} as {{ output_col_alias }}
        from expanded
        where _iter <= {{ max_rows | int }}
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
        with recursive gen as (
            -- base case: one row per input record
            select
                row({{ alias }}.*) as payload,
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