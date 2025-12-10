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
    {# ---------------- basic validations ---------------- #}
    {% if init_expr is none or init_expr == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required") %}
    {% endif %}
    {% if condition_expr is none or condition_expr == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required") %}
    {% endif %}
    {% if loop_expr is none or loop_expr == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required") %}
    {% endif %}

    {# accept string 'None' passed from callers and fallback to default #}
    {% if max_rows is none or max_rows == '' or (max_rows is string and max_rows | lower == 'none') %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set alias = "src" %}
    {% set relation_tables = (relation_name if relation_name is iterable and relation_name is not string else [relation_name]) | join(', ')  %}

    {# unquote user-supplied column name and build internal column #}
    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}

    {# Detect date/timestamp style init expressions (kept from original macro) #}
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

    {# Normalize quotes in condition expression if user used only double quotes #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# --- Prepare quoted variants of column name --- #}
    {% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set backtick_col = "`" ~ unquoted_col ~ "`" %}
    {% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
    {% set singleq_col = "'" ~ unquoted_col ~ "'" %}
    {% set plain_col = unquoted_col %}

    {# ---------------- Safe replacement helper (sentinel padding technique) ----------------
       We will add a sentinel character at both ends of the expression so we can safely
       replace standalone occurrences of the identifier that are bounded by common token
       separators. This avoids matching substrings like `YMD2` or replacing after a dot
       (e.g. `payload.YMD`).
    #}

    {# --- Prepare the condition expression: replace quoted forms first --- #}
    {% set _cond_tmp = condition_expr_sql %}
    {% set _cond_tmp = _cond_tmp | replace(q_by_adapter, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(backtick_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(doubleq_col, internal_col) %}
    {% set _cond_tmp = _cond_tmp | replace(singleq_col, internal_col) %}

    {# --- Now safely replace plain (unquoted) occurrences ONLY when they are standalone tokens.
         We will pad with a sentinel '¦' that is very unlikely to appear in SQL, and perform
         multiple guarded replaces for the common token boundaries. This avoids using regex.
    #}
    {% set _p = '¦' ~ _cond_tmp ~ '¦' %}

    {# boundaries to handle: start/end, space, comma, parens, operators, semicolon, newline, tab #}
    {% set _p = _p | replace('¦' ~ plain_col ~ '¦', '¦' ~ internal_col ~ '¦') %}
    {% set _p = _p | replace('¦' ~ plain_col ~ ' ', '¦' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace(' ' ~ plain_col ~ '¦', ' ' ~ internal_col ~ '¦') %}
    {% set _p = _p | replace('(' ~ plain_col ~ ' ', '(' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace(' ' ~ plain_col ~ ')', ' ' ~ internal_col ~ ')') %}
    {% set _p = _p | replace(',' ~ plain_col ~ ',', ',' ~ internal_col ~ ',') %}
    {% set _p = _p | replace(',' ~ plain_col ~ ' ', ',' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace(' ' ~ plain_col ~ ',', ' ' ~ internal_col ~ ',') %}
    {% set _p = _p | replace('=' ~ plain_col ~ ' ', '=' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace('>' ~ plain_col ~ ' ', '>' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace('<' ~ plain_col ~ ' ', '<' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace('!' ~ plain_col ~ ' ', '!' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace(' ' ~ plain_col ~ ';', ' ' ~ internal_col ~ ';') %}
    {% set _p = _p | replace('\n' ~ plain_col ~ '\n', '\n' ~ internal_col ~ '\n') %}
    {% set _p = _p | replace('\t' ~ plain_col ~ '\t', '\t' ~ internal_col ~ '\t') %}
    {# handle cases where identifier followed/preceded by arithmetic operators or pipes #}
    {% set _p = _p | replace('+' ~ plain_col ~ ' ', '+' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace(' ' ~ plain_col ~ '+', ' ' ~ internal_col ~ '+') %}
    {% set _p = _p | replace('-' ~ plain_col ~ ' ', '-' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace(' ' ~ plain_col ~ '-', ' ' ~ internal_col ~ '-') %}
    {% set _p = _p | replace('*' ~ plain_col ~ ' ', '*' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace(' ' ~ plain_col ~ '*', ' ' ~ internal_col ~ '*') %}
    {% set _p = _p | replace('/' ~ plain_col ~ ' ', '/' ~ internal_col ~ ' ') %}
    {% set _p = _p | replace(' ' ~ plain_col ~ '/', ' ' ~ internal_col ~ '/') %}
    {% set _cond_tmp = (_p | replace('¦', '') ) %}

    {% set condition_expr_sql = _cond_tmp %}

    {# ---------------- Now prepare loop expression similarly, but we want gen.__gen_x form in recursive step --- #}
    {% set _loop_tmp = loop_expr %}
    {% set _loop_tmp = _loop_tmp | replace(q_by_adapter, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(backtick_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(doubleq_col, 'gen.' ~ internal_col) %}
    {% set _loop_tmp = _loop_tmp | replace(singleq_col, 'gen.' ~ internal_col) %}

    {% set _p2 = '¦' ~ _loop_tmp ~ '¦' %}
    {% set _p2 = _p2 | replace('¦' ~ plain_col ~ '¦', '¦' ~ 'gen.' ~ internal_col ~ '¦') %}
    {% set _p2 = _p2 | replace('¦' ~ plain_col ~ ' ', '¦' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace(' ' ~ plain_col ~ '¦', ' ' ~ 'gen.' ~ internal_col ~ '¦') %}
    {% set _p2 = _p2 | replace('(' ~ plain_col ~ ' ', '(' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace(' ' ~ plain_col ~ ')', ' ' ~ 'gen.' ~ internal_col ~ ')') %}
    {% set _p2 = _p2 | replace(',' ~ plain_col ~ ',', ',' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _p2 = _p2 | replace(',' ~ plain_col ~ ' ', ',' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace(' ' ~ plain_col ~ ',', ' ' ~ 'gen.' ~ internal_col ~ ',') %}
    {% set _p2 = _p2 | replace('=' ~ plain_col ~ ' ', '=' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace('>' ~ plain_col ~ ' ', '>' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace('<' ~ plain_col ~ ' ', '<' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace('!' ~ plain_col ~ ' ', '!' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace(' ' ~ plain_col ~ ';', ' ' ~ 'gen.' ~ internal_col ~ ';') %}
    {% set _p2 = _p2 | replace('\n' ~ plain_col ~ '\n', '\n' ~ 'gen.' ~ internal_col ~ '\n') %}
    {% set _p2 = _p2 | replace('\t' ~ plain_col ~ '\t', '\t' ~ 'gen.' ~ internal_col ~ '\t') %}
    {% set _p2 = _p2 | replace('+' ~ plain_col ~ ' ', '+' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace(' ' ~ plain_col ~ '+', ' ' ~ 'gen.' ~ internal_col ~ '+') %}
    {% set _p2 = _p2 | replace('-' ~ plain_col ~ ' ', '-' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace(' ' ~ plain_col ~ '-', ' ' ~ 'gen.' ~ internal_col ~ '-') %}
    {% set _p2 = _p2 | replace('*' ~ plain_col ~ ' ', '*' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace(' ' ~ plain_col ~ '*', ' ' ~ 'gen.' ~ internal_col ~ '*') %}
    {% set _p2 = _p2 | replace('/' ~ plain_col ~ ' ', '/' ~ 'gen.' ~ internal_col ~ ' ') %}
    {% set _p2 = _p2 | replace(' ' ~ plain_col ~ '/', ' ' ~ 'gen.' ~ internal_col ~ '/') %}
    {% set loop_expr_replaced = (_p2 | replace('¦', '') ) %}

    {# Use adapter-safe quoting for EXCEPT column #}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {# --- Build recursion_condition: same condition but referencing the previous iteration as gen.__gen_col --- #}
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

    {# ---------------- Final SQL generation ---------------- #}
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
            -- exclude original column from payload to avoid duplicate column names
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