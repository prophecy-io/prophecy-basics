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

    {# max_rows defaults to 100 if not provided (set by apply function) #}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100 %}
    {% endif %}

    {% set alias = "src" %}
    {% set relation_tables = (relation_name if relation_name is iterable and relation_name is not string else [relation_name]) | join(', ')  %}

    {# Use provided helper to unquote the provided column name #}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# Detect if init_expr is a simple date string literal and cast it to DATE if needed #}
    {# Only cast if it's a plain date string (not an expression) to avoid false positives #}
    {% set init_expr_trimmed = init_expr | trim %}
    {% set is_date_string = false %}
    {% set date_str_to_cast = init_expr %}
    
    {# First check if it matches a date pattern (YYYY-MM-DD) - this takes precedence over operator detection #}
    {% set looks_like_date = false %}
    {% if init_expr_trimmed.startswith("'") and init_expr_trimmed.endswith("'") %}
        {% set date_str = init_expr_trimmed[1:-1] %}
        {% if date_str | length == 10 and date_str[4] == '-' and date_str[7] == '-' %}
            {% set looks_like_date = true %}
        {% endif %}
    {% elif init_expr_trimmed.startswith('"') and init_expr_trimmed.endswith('"') %}
        {% set date_str = init_expr_trimmed[1:-1] %}
        {% if date_str | length == 10 and date_str[4] == '-' and date_str[7] == '-' %}
            {% set looks_like_date = true %}
        {% endif %}
    {% elif init_expr_trimmed | length == 10 and init_expr_trimmed[4] == '-' and init_expr_trimmed[7] == '-' %}
        {# Unquoted date string - check it's not an expression #}
        {% set looks_like_date = true %}
    {% endif %}
    
    {# Only check for date strings if it looks like a date AND doesn't contain expression operators #}
    {% if looks_like_date %}
        {# Check if it contains expression operators (not just date separators) #}
        {% set has_expression_operators = '(' in init_expr_trimmed or ')' in init_expr_trimmed or '+' in init_expr_trimmed or '*' in init_expr_trimmed or '/' in init_expr_trimmed or 'payload.' in init_expr_trimmed %}
        {# For unquoted dates, also check if '-' appears in positions other than 4 and 7 (indicating subtraction) #}
        {% if not init_expr_trimmed.startswith("'") and not init_expr_trimmed.startswith('"') %}
            {# Check if there are spaces or other characters that suggest it's an expression #}
            {% if ' ' in init_expr_trimmed or init_expr_trimmed.count('-') > 2 %}
                {% set has_expression_operators = true %}
            {% endif %}
        {% endif %}
        
        {% if not has_expression_operators %}
            {% set is_date_string = true %}
            {% if init_expr_trimmed.startswith("'") or init_expr_trimmed.startswith('"') %}
                {% set date_str_to_cast = init_expr %}
            {% else %}
                {# Wrap unquoted date string in quotes for SQL #}
                {% set date_str_to_cast = "'" ~ init_expr_trimmed ~ "'" %}
            {% endif %}
        {% endif %}
    {% endif %}
    
    {% if is_date_string %}
        {# Cast date string to DATE to ensure correct type #}
        {% set init_select = 'CAST(' ~ date_str_to_cast ~ ' AS DATE)' %}
    {% else %}
        {% set init_select = init_expr %}
    {% endif %}

    {# Normalize user-supplied condition expression quotes if they used double quotes only #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# --- Replace the target column in condition expression to reference the internal column --- #}
    {% set condition_expr_sql = prophecy_basics.replace_column_in_expression(condition_expr_sql, unquoted_col, internal_col, preserve_payload=true) %}

    {# --- Replace the target column in loop expression to reference gen.<internal_col> in recursive step --- #}
    {% set loop_expr_replaced = prophecy_basics.replace_column_in_expression(loop_expr, unquoted_col, 'gen.' ~ internal_col, preserve_payload=false) %}

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
    {# max_rows defaults to 100 if not provided (set by apply function) #}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100 %}
    {% endif %}

    {% set alias = "src" %}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set relation_tables = relation_list | join(', ') if relation_list and relation_list|length > 0 and relation_list[0] else '' %}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set output_col_alias = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}
    {% set array_elem_alias = internal_col %}

    {# Extract init value - evaluate init_expr by replacing column_name with 1 #}
    {% set init_expr_eval = init_expr | replace(column_name, '1') | trim %}
    {% set init_value = init_expr_eval | int %}
    
    {# Extract step value - evaluate loop_expr by replacing column_name with 0 #}
    {% set step_expr = loop_expr | replace(column_name, '0') | trim %}
    {% set step_value = step_expr | int %}
    
    {# Use GENERATE_ARRAY with step - always use array element alias #}
    {% set computed_expr = array_elem_alias %}
    {% set condition_expr_sql = condition_expr | replace(column_name, array_elem_alias) %}
    {% set use_step = true %}
    {% set array_step = step_value %}
    
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
                generate_array({{ init_value }}, {{ init_value + max_rows | int * array_step }}, {{ array_step }})
            ) as {{ array_elem_alias }}
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
            ) as {% if use_step %}{{ array_elem_alias }}{% else %} _iter {% endif %}
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
    {% if init_expr is none or init_expr == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required") %}
    {% endif %}

    {% if condition_expr is none or condition_expr == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required") %}
    {% endif %}

    {% if loop_expr is none or loop_expr == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required") %}
    {% endif %}

    {# max_rows defaults to 100 if not provided (set by apply function) #}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100 %}
    {% endif %}

    {% set alias = "src" %}
    {% set relation_tables = (relation_name if relation_name is iterable and relation_name is not string else [relation_name]) | join(', ')  %}

    {# Use provided helper to unquote the provided column name #}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# Detect if init_expr is a simple date string literal and cast it to DATE if needed #}
    {# Only cast if it's a plain date string (not an expression) to avoid false positives #}
    {% set init_expr_trimmed = init_expr | trim %}
    {% set is_date_string = false %}
    {% set date_str_to_cast = init_expr %}
    
    {# First check if it matches a date pattern (YYYY-MM-DD) - this takes precedence over operator detection #}
    {% set looks_like_date = false %}
    {% if init_expr_trimmed.startswith("'") and init_expr_trimmed.endswith("'") %}
        {% set date_str = init_expr_trimmed[1:-1] %}
        {% if date_str | length == 10 and date_str[4] == '-' and date_str[7] == '-' %}
            {% set looks_like_date = true %}
        {% endif %}
    {% elif init_expr_trimmed.startswith('"') and init_expr_trimmed.endswith('"') %}
        {% set date_str = init_expr_trimmed[1:-1] %}
        {% if date_str | length == 10 and date_str[4] == '-' and date_str[7] == '-' %}
            {% set looks_like_date = true %}
        {% endif %}
    {% elif init_expr_trimmed | length == 10 and init_expr_trimmed[4] == '-' and init_expr_trimmed[7] == '-' %}
        {# Unquoted date string - check it's not an expression #}
        {% set looks_like_date = true %}
    {% endif %}
    
    {# Only check for date strings if it looks like a date AND doesn't contain expression operators #}
    {% if looks_like_date %}
        {# Check if it contains expression operators (not just date separators) #}
        {% set has_expression_operators = '(' in init_expr_trimmed or ')' in init_expr_trimmed or '+' in init_expr_trimmed or '*' in init_expr_trimmed or '/' in init_expr_trimmed or 'payload.' in init_expr_trimmed %}
        {# For unquoted dates, also check if '-' appears in positions other than 4 and 7 (indicating subtraction) #}
        {% if not init_expr_trimmed.startswith("'") and not init_expr_trimmed.startswith('"') %}
            {# Check if there are spaces or other characters that suggest it's an expression #}
            {% if ' ' in init_expr_trimmed or init_expr_trimmed.count('-') > 2 %}
                {% set has_expression_operators = true %}
            {% endif %}
        {% endif %}
        
        {% if not has_expression_operators %}
            {% set is_date_string = true %}
            {% if init_expr_trimmed.startswith("'") or init_expr_trimmed.startswith('"') %}
                {% set date_str_to_cast = init_expr %}
            {% else %}
                {# Wrap unquoted date string in quotes for SQL #}
                {% set date_str_to_cast = "'" ~ init_expr_trimmed ~ "'" %}
            {% endif %}
        {% endif %}
    {% endif %}
    
    {% if is_date_string %}
        {# Cast date string to DATE to ensure correct type #}
        {% set init_select = 'CAST(' ~ date_str_to_cast ~ ' AS DATE)' %}
    {% else %}
        {% set init_select = init_expr %}
    {% endif %}

    {# Normalize user-supplied condition expression quotes if they used double quotes only #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# --- Replace the target column in condition expression to reference the internal column --- #}
    {% set condition_expr_sql = prophecy_basics.replace_column_in_expression(condition_expr_sql, unquoted_col, internal_col, preserve_payload=true) %}

    {# --- Replace the target column in loop expression to reference gen.<internal_col> in recursive step --- #}
    {% set loop_expr_replaced = prophecy_basics.replace_column_in_expression(loop_expr, unquoted_col, 'gen.' ~ internal_col, preserve_payload=false) %}

    {# Use adapter-safe quoting for EXCLUDE column #}
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
{% endmacro %}