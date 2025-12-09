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
    {# Validate required parameters #}
    {% if init_expr is none or init_expr == '' or condition_expr is none or condition_expr == '' or loop_expr is none or loop_expr == '' or column_name is none or column_name == '' %}
        select 'ERROR: init_expr, condition_expr, loop_expr, and column_name are required and cannot be empty' as error_message
    {% else %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set relation_tables = relation_list | join(', ') if relation_list and relation_list|length > 0 and relation_list[0] else '' %}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set output_col_alias = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}
    {% set array_elem_alias = internal_col %}

    {# Extract init value for array generation #}
    {% set init_value = init_expr | replace(column_name, '0') | trim | int %}
    
    {# Common replacements: column_name -> array_elem_alias for array element reference #}
    {% set loop_expr_val = loop_expr | replace(column_name, array_elem_alias) %}
    {% set condition_expr_val = condition_expr | replace(column_name, array_elem_alias) %}
    
    {# Detect patterns and compute accordingly #}
    {% set loop_expr_clean = loop_expr | trim | replace(' ', '') %}
    {% set col_clean = column_name | trim | replace(' ', '') %}
    
    {# Check for multiplication pattern: val * N -> use POWER(N, iteration) #}
    {% if loop_expr_clean | length > col_clean | length and loop_expr_clean | slice(0, col_clean | length) == col_clean and '*' in loop_expr_clean %}
        {# Extract multiplier from val * N #}
        {% set after_col = loop_expr_clean | slice(col_clean | length) %}
        {% if after_col | slice(0, 1) == '*' %}
            {% set mult_str = after_col | slice(1) | trim %}
            {% if mult_str | int %}
                {% set multiplier = mult_str | int %}
                {# Use POWER for recursive multiplication: init * POWER(multiplier, iteration) #}
                {% set computed_expr = init_value ~ ' * POWER(' ~ multiplier ~ ', _iter)' %}
                {% set condition_expr_sql = condition_expr | replace(column_name, computed_expr) %}
                {% set use_step = false %}
            {% else %}
                {% set computed_expr = loop_expr_val %}
                {% set condition_expr_sql = condition_expr_val %}
                {% set use_step = false %}
            {% endif %}
        {% else %}
            {% set computed_expr = loop_expr_val %}
            {% set condition_expr_sql = condition_expr_val %}
            {% set use_step = false %}
        {% endif %}
    {# Check for addition pattern: val + N -> use GENERATE_ARRAY with step #}
    {% elif loop_expr_clean | length > col_clean | length and loop_expr_clean | slice(0, col_clean | length) == col_clean and '+' in loop_expr_clean %}
        {# Extract step from val + N #}
        {% set after_col = loop_expr_clean | slice(col_clean | length) %}
        {% if after_col | slice(0, 1) == '+' %}
            {% set step_str = after_col | slice(1) | trim %}
            {% if step_str | int %}
                {% set step_value = step_str | int %}
                {# Use GENERATE_ARRAY with step, then just select array element #}
                {% set computed_expr = array_elem_alias %}
                {% set condition_expr_sql = condition_expr_val %}
                {% set use_step = true %}
                {% set array_step = step_value %}
            {% else %}
                {% set computed_expr = loop_expr_val %}
                {% set condition_expr_sql = condition_expr_val %}
                {% set use_step = false %}
            {% endif %}
        {% else %}
            {% set computed_expr = loop_expr_val %}
            {% set condition_expr_sql = condition_expr_val %}
            {% set use_step = false %}
        {% endif %}
    {% else %}
        {# For other expressions, use direct transformation #}
        {% set computed_expr = loop_expr_val %}
        {% set condition_expr_sql = condition_expr_val %}
        {% set use_step = false %}
    {% endif %}
    
    {% if relation_tables %}
        {# With source table: generate sequence for each input row #}
        with source_data as (
            select {{ alias }}.*
            from {{ relation_tables }} {{ alias }}
        ),
        gen as (
            select
                source_data.*,
                {{ computed_expr }} as {{ output_col_alias }}
            from source_data
            cross join unnest(
                {% if use_step %}
                    generate_array({{ init_value }}, {{ init_value + max_rows | int * array_step }}, {{ array_step }})
                {% else %}
                    generate_array(0, {{ max_rows | int }} - 1)
                {% endif %}
            ) as {% if use_step %}{{ array_elem_alias }}{% else %} _iter {% endif %}
            where {{ condition_expr_sql }}
            limit {{ max_rows | int }}
        )
        select gen.*
        from gen
    {% else %}
        {# Without source table: simple sequence generation #}
        with gen as (
            select {{ computed_expr }} as {{ output_col_alias }}
            from unnest(
                {% if use_step %}
                    generate_array({{ init_value }}, {{ init_value + max_rows | int * array_step }}, {{ array_step }})
                {% else %}
                    generate_array(0, {{ max_rows | int }} - 1)
                {% endif %}
            ) as {% if use_step %}{{ array_elem_alias }}{% else %}_iter{% endif %}
            where {{ condition_expr_sql }}
            limit {{ max_rows | int }}
        )
        select {{ output_col_alias }}
        from gen
    {% endif %}
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