{#
  GenerateRows Macro Gem
  ======================

  Expands each row into a sequence of values you define—counts, dates, or other
  stepped values—either from a seed table or from scratch. You supply start,
  condition, and step logic; the macro fills in the repeating column until your
  rule stops.

  Parameters:
    - relation_name (list): Source relation identifier(s); use `[]` for generator-only (no source table).
    - schema: Used with prophecy_basics.column_exists_in_schema for EXCEPT of original column.
    - init_expr, condition_expr, loop_expr: Generator expressions (column_name references the iterated column).
    - column_name: Logical name of the generated column (unquoted for internal __gen_ col).
    - max_rows: Recursion cap (default 100000; empty coerces to 100).
    - force_mode: Reserved (default 'recursive'); currently ignored by every adapter.

  Adapter Support:
    - default__ (recursive CTE, struct payload, EXCEPT)
    - duckdb__ (recursive CTE, flattened payload, EXCLUDE)
    - snowflake__ (recursive CTE; source columns are listed from `schema`, since Snowflake
      requires an explicit column list on recursive CTEs)
    - bigquery__ (no recursion: GENERATE_ARRAY offsets; loop_expr must have a constant step,
      i.e. `col + step`, `col - step` or DATE_ADD/DATE_SUB/DATETIME_*/TIMESTAMP_*(col, INTERVAL step part))

  Depends on schema parameter:
    Yes (required on Snowflake when a source relation is given)

  Macro Call Examples (default__):
    {{ prophecy_basics.GenerateRows([ref('seed')], schema, '1', 'value <= 10', 'value + 1', 'value', 1000, 'recursive') }}
    {{ prophecy_basics.GenerateRows([], '[]', '1', 'n <= 5', 'n + 1', 'n', 100, 'recursive') }}

  CTE Usage Example:
    Macro call (second example above — standalone sequence, no source table):
      {{ prophecy_basics.GenerateRows([], '[]', '1', 'n <= 5', 'n + 1', 'n', 100, 'recursive') }}

    Resolved query (default__ — illustrative; internal column is `__gen_n`):
      with recursive gen as (
          select 1 as __gen_n, 1 as _iter
          union all
          select
              gen.__gen_n + 1 as __gen_n,
              _iter + 1
          from gen
          where _iter < 100 and (gen.__gen_n <= 5)
      )
      select __gen_n as n
      from gen
      where __gen_n <= 5
#}
{% macro GenerateRows(relation_name,
    schema,
    init_expr,
    condition_expr,
    loop_expr,
    column_name,
    max_rows,
    force_mode) -%}
    {{ return(adapter.dispatch('GenerateRows', 'prophecy_basics')(relation_name,
    schema,
    init_expr,
    condition_expr,
    loop_expr,
    column_name,
    max_rows,
    force_mode)) }}
{% endmacro %}

{% macro default__GenerateRows(
    relation_name=None,
    schema='[]',
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

    {# Check if column exists in source schema #}
    {% set column_exists_in_schema = prophecy_basics.column_exists_in_schema(schema, column_name) %}

    {# Detect if init_expr is a simple date or timestamp string literal and cast it if needed #}
    {% set init_select = prophecy_basics.cast_timestamp_if_needed(init_expr) %}

    {# Normalize user-supplied condition expression quotes if they used double quotes only #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# --- Replace the target column in condition expression to reference the internal column --- #}
    {% set condition_expr_sql = prophecy_basics.replace_column_in_expression(condition_expr_sql, unquoted_col, internal_col, preserve_payload=true) %}

    {# --- Replace the target column in loop expression to reference gen.<internal_col> in recursive step --- #}
    {# Properly quote internal_col when creating gen. reference (e.g., gen.`__gen_date_val` not gen.__gen_date_val) #}
    {# Use backticks for internal columns to ensure proper SQL syntax #}
    {% set quoted_internal_col = "`" ~ internal_col ~ "`" %}
    {% set loop_expr_replaced = prophecy_basics.replace_column_in_expression(loop_expr, unquoted_col, 'gen.' ~ quoted_internal_col, preserve_payload=false) %}

    {# Use adapter-safe quoting for EXCEPT column #}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {# --- Build recursion_condition: same condition but referencing the previous iteration (gen.__gen_col) --- #}
    {% set _rec_tmp = condition_expr_sql %}
    {% set _rec_tmp = _rec_tmp | replace(internal_col, 'gen.' ~ quoted_internal_col) %}
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
            -- Exclude the original column from payload only if it exists in the source schema
            {% if column_exists_in_schema %}
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
    schema='[]',
    init_expr='1',
    condition_expr='value <= 10',
    loop_expr='value + 1',
    column_name='value',
    max_rows=100000,
    force_mode='recursive'
) %}
    {#
        BigQuery rejects WITH RECURSIVE anywhere but the top level of a statement, and Prophecy
        nests every gems SQL inside a CTE, so this adapter cannot recurse. Instead each value is
        computed from its offset k (init + k * step, or DATE_ADD(init, INTERVAL k * step part)),
        which needs loop_expr to have a constant step. MONTH/QUARTER/YEAR steps are therefore
        anchored to init (Jan 31 -> Feb 28/29 -> Mar 31) rather than to the previous value.
    #}
    {% for param_name, param_value in [('init_expr', init_expr), ('condition_expr', condition_expr), ('loop_expr', loop_expr), ('column_name', column_name)] %}
        {% if param_value is none or param_value | trim == '' %}
            {% do exceptions.raise_compiler_error(param_name ~ " is required") %}
        {% endif %}
    {% endfor %}

    {# max_rows defaults to 100 if not provided (set by apply function) #}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100 %}
    {% endif %}
    {% set max_rows = max_rows | int %}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set relation_tables = relation_list | reject('none') | reject('equalto', '') | join(', ') %}

    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set allowed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
    {% set ns = namespace(safe_col='', has_specials=false) %}
    {% for ch in unquoted_col %}
        {% if ch in allowed %}
            {% set ns.safe_col = ns.safe_col ~ ch %}
        {% else %}
            {% set ns.safe_col = ns.safe_col ~ '_' %}
            {% set ns.has_specials = true %}
        {% endif %}
    {% endfor %}
    {% set output_col_alias = (prophecy_basics.quote_identifier(unquoted_col) | trim) if ns.has_specials else unquoted_col %}
    {% set except_col = prophecy_basics.quote_identifier(unquoted_col) | trim %}
    {% set gen_col = '__gen_' ~ ns.safe_col %}

    {% set column_exists_in_schema = prophecy_basics.column_exists_in_schema(schema, column_name) %}
    {% set init_select = prophecy_basics.cast_timestamp_if_needed(init_expr) %}

    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}
    {% set condition_sql = prophecy_basics.replace_column_in_expression(condition_expr_sql, unquoted_col, gen_col, preserve_payload=true) %}

    {# --- Closed form of loop_expr: value at offset {idx} --- #}
    {% set re = modules.re %}
    {% set col_re = '(?:' ~ re.escape(unquoted_col) ~ '|`' ~ re.escape(unquoted_col) ~ '`)' %}
    {% set step_re = '(\\d+(?:\\.\\d+)?|payload\\.(?:\\w+|`[^`]+`)|\\(.*\\))' %}
    {% set date_part_re = '((?i:MICROSECOND|MILLISECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|QUARTER|YEAR))' %}
    {% set arithmetic_step = re.match('^\\s*' ~ col_re ~ '\\s*([+-])\\s*' ~ step_re ~ '\\s*$', loop_expr) %}
    {% set interval_step = re.match('^\\s*((?i:DATETIME|DATE|TIMESTAMP)_(?i:ADD|SUB))\\s*\\(\\s*' ~ col_re ~ '\\s*,\\s*(?i:INTERVAL)\\s+' ~ step_re ~ '\\s+' ~ date_part_re ~ '\\s*\\)\\s*$', loop_expr) %}

    {% set value_template = none %}
    {% if arithmetic_step %}
        {% set step_sql = arithmetic_step.group(2) %}
        {% set value_template = '(' ~ init_select ~ ') ' ~ arithmetic_step.group(1) ~ ' {idx} * (' ~ step_sql ~ ')' %}
    {% elif interval_step %}
        {% set step_sql = interval_step.group(2) %}
        {% set value_template = interval_step.group(1) ~ '(' ~ init_select ~ ', INTERVAL {idx} * (' ~ step_sql ~ ') ' ~ interval_step.group(3) ~ ')' %}
    {% endif %}
    {% if value_template is not none and gen_col in prophecy_basics.replace_column_in_expression(step_sql, unquoted_col, gen_col, preserve_payload=true) %}
        {% set value_template = none %}
    {% endif %}
    {% if value_template is none %}
        {% do exceptions.raise_compiler_error(
            "GenerateRows on BigQuery needs a loop expression with a constant step: '" ~ unquoted_col ~ " + <step>', '"
            ~ unquoted_col ~ " - <step>' or DATE_ADD/DATE_SUB/DATETIME_ADD/DATETIME_SUB/TIMESTAMP_ADD/TIMESTAMP_SUB("
            ~ unquoted_col ~ ", INTERVAL <step> <part>), where <step> is a number, a payload column or a parenthesized "
            ~ "expression that does not use " ~ unquoted_col ~ ". Got: " ~ loop_expr
        ) %}
    {% endif %}

    {#
        _gen_count is how many leading offsets satisfy the condition, so the output stops at the
        first failing value exactly like the recursive adapters do (rows are not filtered).
    #}
    {% set value_at_offset = value_template | replace('{idx}', '_gen_idx') %}
    {% set condition_at_probe = condition_sql | replace(gen_col, '(' ~ (value_template | replace('{idx}', '_gen_probe')) ~ ')') %}
    {% set count_sql %}
        ifnull((
            select min(_gen_probe)
            from unnest(generate_array(0, {{ max_rows - 1 }})) as _gen_probe
            where not coalesce({{ condition_at_probe }}, false)
        ), {{ max_rows }})
    {% endset %}

    {% if relation_tables %}
        select
            payload.* except (_gen_count{% if column_exists_in_schema %}, {{ except_col }}{% endif %}),
            {{ value_at_offset }} as {{ output_col_alias }}
        from (
            select payload.*, {{ count_sql }} as _gen_count
            from {{ relation_tables }} payload
        ) payload
        cross join unnest(generate_array(0, payload._gen_count - 1)) as _gen_idx
    {% else %}
        select {{ value_at_offset }} as {{ output_col_alias }}
        from (select {{ count_sql }} as _gen_count) payload
        cross join unnest(generate_array(0, payload._gen_count - 1)) as _gen_idx
    {% endif %}
{% endmacro %}

{% macro snowflake__GenerateRows(
    relation_name=None,
    schema='[]',
    init_expr='1',
    condition_expr='value <= 10',
    loop_expr='value + 1',
    column_name='value',
    max_rows=100000,
    force_mode='recursive'
) %}
    {% for param_name, param_value in [('init_expr', init_expr), ('condition_expr', condition_expr), ('loop_expr', loop_expr), ('column_name', column_name)] %}
        {% if param_value is none or param_value | trim == '' %}
            {% do exceptions.raise_compiler_error(param_name ~ " is required") %}
        {% endif %}
    {% endfor %}

    {# max_rows defaults to 100 if not provided (set by apply function) #}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100 %}
    {% endif %}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set relation_tables = relation_list | reject('none') | reject('equalto', '') | join(', ') %}

    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
    {% set allowed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
    {% set ns = namespace(safe_col='', has_specials=false) %}
    {% for ch in unquoted_col %}
        {% if ch in allowed %}
            {% set ns.safe_col = ns.safe_col ~ ch %}
        {% else %}
            {% set ns.safe_col = ns.safe_col ~ '_' %}
            {% set ns.has_specials = true %}
        {% endif %}
    {% endfor %}
    {# Unquoted aliases resolve to upper case, matching how downstream SQL refers to them #}
    {% set output_col_alias = (prophecy_basics.quote_identifier(unquoted_col) | trim) if ns.has_specials else unquoted_col %}
    {% set gen_col = '__gen_' ~ ns.safe_col %}

    {#
        Both the anchor source and the recursive reference are aliased `payload`, so user
        expressions such as payload.qty resolve in every clause; payload.<gen_col> is the
        previous generated value inside the recursive step.
    #}
    {% set prev_value = 'payload.' ~ gen_col %}
    {% set init_select = prophecy_basics.cast_timestamp_if_needed(init_expr) %}

    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}
    {% set condition_sql = prophecy_basics.replace_column_in_expression(condition_expr_sql, unquoted_col, prev_value, preserve_payload=true) %}
    {% set loop_sql = prophecy_basics.replace_column_in_expression(loop_expr, unquoted_col, prev_value, preserve_payload=true) %}

    {% if relation_tables %}
        {# Snowflake requires an explicit column list on recursive CTEs, so source columns come from schema #}
        {% set schema_parsed = fromjson(schema) if schema is string else schema %}
        {% if schema_parsed is none or schema_parsed is string or schema_parsed | length == 0 %}
            {% do exceptions.raise_compiler_error("GenerateRows on Snowflake needs the input schema (a JSON list of {name, dataType}) when a source relation is given") %}
        {% endif %}
        {% set carried_cols = [] %}
        {% set output_cols = [] %}
        {% for c in schema_parsed %}
            {% set quoted_col = prophecy_basics.quote_identifier(c.name) | trim %}
            {% do carried_cols.append(quoted_col) %}
            {% if not prophecy_basics.column_names_match(c.name, unquoted_col) %}
                {% do output_cols.append(quoted_col) %}
            {% endif %}
        {% endfor %}

        with recursive gen ({{ carried_cols | join(', ') }}, {{ gen_col }}, _gen_iter) as (
            select
                {% for col in carried_cols %}payload.{{ col }}, {% endfor %}
                {{ init_select }},
                1
            from {{ relation_tables }} payload

            union all

            select
                {% for col in carried_cols %}payload.{{ col }}, {% endfor %}
                {{ loop_sql }},
                payload._gen_iter + 1
            from gen payload
            where payload._gen_iter < {{ max_rows | int }}
              and ({{ condition_sql }})
        )
        select
            {% for col in output_cols %}payload.{{ col }}, {% endfor %}
            payload.{{ gen_col }} as {{ output_col_alias }}
        from gen payload
        where {{ condition_sql }}
    {% else %}
        with recursive gen ({{ gen_col }}, _gen_iter) as (
            select {{ init_select }}, 1

            union all

            select
                {{ loop_sql }},
                payload._gen_iter + 1
            from gen payload
            where payload._gen_iter < {{ max_rows | int }}
              and ({{ condition_sql }})
        )
        select payload.{{ gen_col }} as {{ output_col_alias }}
        from gen payload
        where {{ condition_sql }}
    {% endif %}
{% endmacro %}

{% macro duckdb__GenerateRows(
    relation_name=None,
    schema='[]',
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

    {# Check if column exists in source schema #}
    {% set column_exists_in_schema = prophecy_basics.column_exists_in_schema(schema, column_name) %}

    {# Detect if init_expr is a simple date or timestamp string literal and cast it if needed #}
    {% set init_select = prophecy_basics.cast_timestamp_if_needed(init_expr) %}

    {# Normalize user-supplied condition expression quotes if they used double quotes only #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# --- Replace the target column in condition expression to reference the internal column --- #}
    {% set condition_expr_sql = prophecy_basics.replace_column_in_expression(condition_expr_sql, unquoted_col, internal_col, preserve_payload=true) %}

    {# --- Replace the target column in loop expression to reference gen.<internal_col> in recursive step --- #}
    {# Properly quote internal_col when creating gen. reference (e.g., gen.`__gen_date_val` not gen.__gen_date_val) #}
    {# Use backticks for internal columns to ensure proper SQL syntax #}
    {% set quoted_internal_col = internal_col %}
    {% set loop_expr_replaced = prophecy_basics.replace_column_in_expression(loop_expr, unquoted_col, 'gen.' ~ quoted_internal_col, preserve_payload=false) %}

    {# Use adapter-safe quoting for EXCLUDE column #}
    {% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}

    {# --- Build recursion_condition: same condition but referencing the previous iteration (gen.__gen_col) --- #}
    {% set _rec_tmp = condition_expr_sql %}
    {% set _rec_tmp = _rec_tmp | replace(internal_col, 'gen.' ~ quoted_internal_col) %}
    {# Note: condition_expr_sql already has internal_col substituted; above we switch to gen.internal_col for recursive WHERE #}
    {% set recursion_condition = _rec_tmp %}
    {% set loop_expr_replaced_for_gen = loop_expr_replaced | replace('payload.', 'gen.') %}
    {% set recursion_condition_for_gen = recursion_condition | replace('payload.', 'gen.') %}
    {% set condition_expr_sql_for_gen = condition_expr_sql | replace('payload.', 'gen.') %}

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
            -- Don't exclude the column here - it needs to be available in the recursive loop
            select
                payload.*,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from payload

            union all

            -- recursive step
            select
                gen.* EXCLUDE (_iter, {{ internal_col }}),
                {{ loop_expr_replaced_for_gen }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
              and ({{ recursion_condition_for_gen }})
        )
        select
            -- Exclude internal column, iteration counter, and original column from payload (only if it exists in schema)
            {% if column_exists_in_schema %}
            gen.* EXCLUDE ({{ internal_col }}, _iter, {{ unquoted_col }}),
            {% else %}
            gen.* EXCLUDE ({{ internal_col }}, _iter),
            {% endif %}
            {{ internal_col }} as {{ output_col_alias }}
        from gen
        where {{ condition_expr_sql_for_gen }}
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