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
    {# Validate required parameters #}
    {% if init_expr is none or init_expr == '' or condition_expr is none or condition_expr == '' or loop_expr is none or loop_expr == '' or column_name is none or column_name == '' %}
        select 'ERROR: init_expr, condition_expr, loop_expr, and column_name are required and cannot be empty' as error_message
    {% else %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {% set relation_tables = (relation_name if relation_name is iterable and relation_name is not string else [relation_name]) | join(', ')  %}
    {# Use provided helper to unquote the provided column name #}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# Use expressions directly and replace column_name with internal_col #}
    {% set init_select = init_expr | replace(column_name, internal_col) %}
    {% set condition_expr_sql = condition_expr | replace(column_name, internal_col) %}
    {% set loop_expr_replaced = loop_expr | replace(column_name, 'gen.' ~ internal_col) %}
    {# Build recursion_condition: same condition but referencing previous iteration #}
    {% set recursion_condition = condition_expr_sql | replace(internal_col, 'gen.' ~ internal_col) %}
    {% set output_col_alias = column_name %}
    {% if relation_tables %}
        with recursive gen as (
            -- base case: one row per input record
            select
                {{ alias }}.*,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from {{ relation_tables }} {{ alias }}

            union all

            -- recursive step
            select
                gen.* EXCEPT ({{ internal_col }}, _iter),
                {{ loop_expr_replaced }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition }})
        )
        select
            gen.* EXCEPT ({{ internal_col }}, _iter),
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
    {# Just delegate to the default recursive implementation #}
    {{ default__GenerateRows(relation_name, init_expr, condition_expr, loop_expr, column_name, max_rows, force_mode) }}
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
    {# Validate required parameters #}
    {% if init_expr is none or init_expr == '' or condition_expr is none or condition_expr == '' or loop_expr is none or loop_expr == '' or column_name is none or column_name == '' %}
        select 'ERROR: init_expr, condition_expr, loop_expr, and column_name are required and cannot be empty' as error_message
    {% else %}
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
    {# Replace payload. references with gen. in recursion condition since payload columns are flattened into gen #}
    {% set recursion_condition = condition_expr_sql | replace(internal_col, 'gen.' ~ internal_col) | replace('payload.', 'gen.') %}
    {# Replace payload. references in final WHERE clause since columns are flattened #}
    {% set condition_expr_sql = condition_expr_sql | replace('payload.', '') %}
    {% set output_col_alias = column_name %}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {% if relation_tables %}
        with recursive payload as (
            -- Select all columns from source table
            select *
            from {{ relation_tables }} {{ alias }}
        ),
        gen as (
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
            -- Exclude internal_col and _iter. Note: We don't exclude column_name here because
            -- DuckDB errors if we try to EXCLUDE a non-existent column. If column_name exists
            -- in source, it will be included and then we'll add it again, causing a duplicate
            -- column error which is expected (user shouldn't have a column with same name as output)
            gen.* EXCLUDE ({{ internal_col }}, _iter),
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
    {% endif %}
{% endmacro %}