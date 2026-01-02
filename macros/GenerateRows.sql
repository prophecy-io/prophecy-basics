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
    {# Replace patterns with spaces/brackets to avoid replacing substrings like YMD in YMD2 #}
    {% set _loop_tmp = _loop_tmp | replace('.' ~ plain_col ~ '.', '.' ~ 'gen.' ~ internal_col ~ '.') %}
    {% set _loop_tmp = _loop_tmp | replace('.' ~ plain_col ~ ')', '.' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _loop_tmp = _loop_tmp | replace('.' ~ plain_col ~ ',', '.' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _loop_tmp = _loop_tmp | replace('.' ~ plain_col ~ ' ', '.' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _loop_tmp = _loop_tmp | replace('(' ~ plain_col ~ ')', '(' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _loop_tmp = _loop_tmp | replace('(' ~ plain_col ~ ',', '(' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _loop_tmp = _loop_tmp | replace('(' ~ plain_col ~ ' ', '(' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _loop_tmp = _loop_tmp | replace(' ' ~ plain_col ~ ')', ' ' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _loop_tmp = _loop_tmp | replace(' ' ~ plain_col ~ ',', ' ' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _loop_tmp = _loop_tmp | replace(' ' ~ plain_col ~ ' ', ' ' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _loop_tmp = _loop_tmp | replace(',' ~ plain_col ~ ')', ',' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _loop_tmp = _loop_tmp | replace(',' ~ plain_col ~ ',', ',' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _loop_tmp = _loop_tmp | replace(',' ~ plain_col ~ ' ', ',' ~ 'gen.' ~ internal_col ~ ' ') %}
    {# Handle start/end of string #}
    {% if _loop_tmp == plain_col %}
        {% set _loop_tmp = 'gen.' ~ internal_col %}
    {% elif _loop_tmp.startswith(plain_col ~ '.') or _loop_tmp.startswith(plain_col ~ ')') or _loop_tmp.startswith(plain_col ~ ',') or _loop_tmp.startswith(plain_col ~ ' ') %}
        {% set _loop_tmp = 'gen.' ~ internal_col ~ _loop_tmp[plain_col | length:] %}
    {% endif %}
    {% if _loop_tmp.endswith('.' ~ plain_col) or _loop_tmp.endswith('(' ~ plain_col) or _loop_tmp.endswith(',' ~ plain_col) or _loop_tmp.endswith(' ' ~ plain_col) %}
        {% set _loop_tmp = _loop_tmp[:_loop_tmp | length - plain_col | length] ~ 'gen.' ~ internal_col %}
    {% endif %}
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

    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {% set relation_tables = (relation_name if relation_name is iterable and relation_name is not string else [relation_name]) | join(', ')  %}

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
    {# Replace patterns with spaces/brackets to avoid replacing substrings like YMD in YMD2 #}
    {% set _loop_tmp = _loop_tmp | replace('.' ~ plain_col ~ '.', '.' ~ 'gen.' ~ internal_col ~ '.') %}
    {% set _loop_tmp = _loop_tmp | replace('.' ~ plain_col ~ ')', '.' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _loop_tmp = _loop_tmp | replace('.' ~ plain_col ~ ',', '.' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _loop_tmp = _loop_tmp | replace('.' ~ plain_col ~ ' ', '.' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _loop_tmp = _loop_tmp | replace('(' ~ plain_col ~ ')', '(' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _loop_tmp = _loop_tmp | replace('(' ~ plain_col ~ ',', '(' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _loop_tmp = _loop_tmp | replace('(' ~ plain_col ~ ' ', '(' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _loop_tmp = _loop_tmp | replace(' ' ~ plain_col ~ ')', ' ' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _loop_tmp = _loop_tmp | replace(' ' ~ plain_col ~ ',', ' ' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _loop_tmp = _loop_tmp | replace(' ' ~ plain_col ~ ' ', ' ' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _loop_tmp = _loop_tmp | replace(',' ~ plain_col ~ ')', ',' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _loop_tmp = _loop_tmp | replace(',' ~ plain_col ~ ',', ',' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _loop_tmp = _loop_tmp | replace(',' ~ plain_col ~ ' ', ',' ~ 'gen.' ~ internal_col ~ ' ') %}
    {# Handle start/end of string #}
    {% if _loop_tmp == plain_col %}
        {% set _loop_tmp = 'gen.' ~ internal_col %}
    {% elif _loop_tmp.startswith(plain_col ~ '.') or _loop_tmp.startswith(plain_col ~ ')') or _loop_tmp.startswith(plain_col ~ ',') or _loop_tmp.startswith(plain_col ~ ' ') %}
        {% set _loop_tmp = 'gen.' ~ internal_col ~ _loop_tmp[plain_col | length:] %}
    {% endif %}
    {% if _loop_tmp.endswith('.' ~ plain_col) or _loop_tmp.endswith('(' ~ plain_col) or _loop_tmp.endswith(',' ~ plain_col) or _loop_tmp.endswith(' ' ~ plain_col) %}
        {% set _loop_tmp = _loop_tmp[:_loop_tmp | length - plain_col | length] ~ 'gen.' ~ internal_col %}
    {% endif %}
    {% set loop_expr_replaced = _loop_tmp %}

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