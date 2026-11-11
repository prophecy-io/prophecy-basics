{#
  DataValidation Macro Gem
  ========================

  Runs data quality checks on every row of the input and decides what flows out of the
  gem's single output port: the rows that pass, the rows that fail (with the names of the
  checks they failed), every row with its results, or a one-row-per-check summary.
  Similar to the Guardrails operator in Databricks Designer.

  Parameters:
    - relation_name (list): The gem's input relation (exactly one).
    - schema (string): JSON list of {name, dataType} for the input. Only needed by the
        "required_columns" check, which runs at compile time against this schema.
    - rules (list of dict): The checks, in order. Every dict has 'checkType'; other keys depend on it:
        not_null           columns                    every listed column has a value
        unique             columns                    the combination of the columns appears once
                                                      (rows with a NULL in a key column pass)
        accepted_values    columns, values            the column text is one of values
        disallowed_values  columns, values            the column text is none of values
        value_range        columns, minValue/maxValue min <= column <= max (either bound optional)
        regex_match        columns, pattern           the whole column text matches pattern
        expression         expression                 a SQL condition that must be true
        required_columns   columns                    the input has every listed column
        row_count          minValue/maxValue          the input row count is within the bounds
      'values' is a comma-separated string. 'checkName' (optional) is the label used in the
      output; it defaults to the check type and columns. Except for not_null and expression,
      a NULL value passes the check; add a not_null check to reject NULLs.
    - output_mode (string):
        'passed'  - rows that pass every check (the input columns only)
        'failed'  - rows that fail at least one check, plus dq_failed_checks
        'all'     - every row, plus dq_passed and dq_failed_checks
        'summary' - one row per check: check_number, check_name, check_type, failed_rows, check_passed
    - on_failure (string):
        'continue' - output according to output_mode
        'fail'     - if any check fails, stop the query with an error (a missing required column
                     stops it at compile time)

  Adapter Support:
    - default__ (Databricks / Spark), duckdb__, snowflake__, bigquery__ (helper macros below differ per adapter)

  Depends on schema parameter:
    Only for the required_columns check

  Macro Call Examples:
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "order_id", "dataType": "Integer"}]',
        [{'checkType': 'not_null', 'columns': ['order_id']},
         {'checkType': 'unique', 'columns': ['order_id']},
         {'checkType': 'accepted_values', 'columns': ['status'], 'values': 'open, shipped, cancelled'}],
        'passed', 'continue') }}

    {{ prophecy_basics.DataValidation(['orders'], '[]',
        [{'checkType': 'value_range', 'columns': ['amount'], 'minValue': '0', 'maxValue': '10000'},
         {'checkType': 'row_count', 'minValue': '1'}],
        'failed', 'fail') }}

  CTE Usage Example:
    Macro call (first example above), resolved query (default__):
      with dq_flagged as (
          select *,
              coalesce((`order_id` is not null), false) as _dq_check_1,
              coalesce(((`order_id` is null) or count(*) over (partition by `order_id`) = 1), false) as _dq_check_2,
              coalesce(((`status` is null or cast(`status` as string) in ('open', 'shipped', 'cancelled'))), false) as _dq_check_3
          from orders
      ),
      dq_evaluated as (
          select *, (_dq_check_1 and _dq_check_2 and _dq_check_3) as _dq_passed, ... as _dq_failed_checks
          from dq_flagged
      ),
      dq_result as (select * from dq_evaluated)
      select * except (_dq_check_1, _dq_check_2, _dq_check_3, _dq_passed, _dq_failed_checks)
      from dq_result
      where _dq_passed
#}
{% macro DataValidation(relation_name, schema, rules, output_mode='passed', on_failure='continue') -%}
    {{ return(adapter.dispatch('DataValidation', 'prophecy_basics')(relation_name, schema, rules, output_mode, on_failure)) }}
{%- endmacro %}


{% macro default__DataValidation(relation_name, schema, rules, output_mode='passed', on_failure='continue') %}
    {%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
    {%- set relation_tables = relation_list | reject('none') | reject('equalto', '') | list -%}
    {%- if relation_tables | length != 1 -%}
        {%- do exceptions.raise_compiler_error("DataValidation needs exactly one input table") -%}
    {%- endif -%}
    {%- set source_table = relation_tables[0] -%}

    {#- Helpers are looked up through dispatch so this works both inside the prophecy_basics
        package and when the file is copied into another project. -#}
    {%- set bare_name = adapter.dispatch('data_validation_bare_name', 'prophecy_basics') -%}
    {%- set quote_column = adapter.dispatch('data_validation_quote_column', 'prophecy_basics') -%}
    {%- set quote_text = adapter.dispatch('data_validation_quote', 'prophecy_basics') -%}
    {%- set to_text = adapter.dispatch('data_validation_to_text', 'prophecy_basics') -%}
    {%- set full_match = adapter.dispatch('data_validation_full_match', 'prophecy_basics') -%}
    {%- set raise_if = adapter.dispatch('data_validation_raise_if', 'prophecy_basics') -%}
    {%- set star_except = adapter.dispatch('data_validation_star_except', 'prophecy_basics') -%}

    {%- if output_mode not in ['passed', 'failed', 'all', 'summary'] -%}
        {%- do exceptions.raise_compiler_error("DataValidation: output_mode must be passed, failed, all or summary, got: " ~ output_mode) -%}
    {%- endif -%}
    {%- if on_failure not in ['continue', 'fail'] -%}
        {%- do exceptions.raise_compiler_error("DataValidation: on_failure must be continue or fail, got: " ~ on_failure) -%}
    {%- endif -%}

    {%- set rule_list = fromjson(rules) if rules is string else rules -%}
    {%- if rule_list is none or rule_list is mapping or rule_list is not iterable or rule_list | length == 0 -%}
        {%- do exceptions.raise_compiler_error("DataValidation: add at least one check") -%}
    {%- endif -%}

    {%- set type_labels = {
        'not_null': 'Not null',
        'unique': 'Unique',
        'accepted_values': 'Accepted values',
        'disallowed_values': 'Disallowed values',
        'value_range': 'Value range',
        'regex_match': 'Pattern match',
        'expression': 'Expression',
        'required_columns': 'Required columns',
        'row_count': 'Row count'
    } -%}
    {%- set number_re = '^-?[0-9]+([.][0-9]+)?$' -%}

    {%- set checks = [] -%}
    {%- set guards = [] -%}
    {%- for rule in rule_list -%}
        {%- set check_type = rule.get('checkType', '') -%}
        {%- set position = 'check ' ~ loop.index ~ ' (' ~ check_type ~ ')' -%}
        {%- if check_type not in type_labels -%}
            {%- do exceptions.raise_compiler_error("DataValidation: unknown check type in check " ~ loop.index ~ ": " ~ check_type) -%}
        {%- endif -%}
        {%- set columns = rule.get('columns') or [] -%}
        {%- set columns = [columns] if columns is string else columns -%}
        {%- set quoted = [] -%}
        {%- for col in columns -%}
            {%- do quoted.append(quote_column(col)) -%}
        {%- endfor -%}
        {%- if check_type not in ['expression', 'row_count'] and quoted | length == 0 -%}
            {%- do exceptions.raise_compiler_error("DataValidation: " ~ position ~ " needs at least one column") -%}
        {%- endif -%}
        {%- set min_value = '' if rule.get('minValue') is none else rule.get('minValue') | string | trim -%}
        {%- set max_value = '' if rule.get('maxValue') is none else rule.get('maxValue') | string | trim -%}
        {%- set parts = [] -%}
        {%- set name_suffix = namespace(text='') -%}

        {%- if check_type == 'not_null' -%}
            {%- for q in quoted -%}
                {%- do parts.append(q ~ ' is not null') -%}
            {%- endfor -%}
            {%- set check_sql = parts | join(' and ') -%}

        {%- elif check_type == 'unique' -%}
            {%- for q in quoted -%}
                {%- do parts.append(q ~ ' is null') -%}
            {%- endfor -%}
            {%- set check_sql = '(' ~ (parts | join(' or ')) ~ ') or count(*) over (partition by ' ~ (quoted | join(', ')) ~ ') = 1' -%}

        {%- elif check_type in ['accepted_values', 'disallowed_values'] -%}
            {%- set literals = [] -%}
            {%- for v in ((rule.get('values') or '') | string).split(',') -%}
                {%- if v | trim != '' -%}
                    {%- do literals.append(quote_text(v | trim)) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if literals | length == 0 -%}
                {%- do exceptions.raise_compiler_error("DataValidation: " ~ position ~ " needs at least one value") -%}
            {%- endif -%}
            {%- set operator = 'in' if check_type == 'accepted_values' else 'not in' -%}
            {%- for q in quoted -%}
                {%- do parts.append('(' ~ q ~ ' is null or ' ~ to_text(q) ~ ' ' ~ operator ~ ' (' ~ (literals | join(', ')) ~ '))') -%}
            {%- endfor -%}
            {%- set check_sql = parts | join(' and ') -%}

        {%- elif check_type == 'value_range' -%}
            {%- if min_value == '' and max_value == '' -%}
                {%- do exceptions.raise_compiler_error("DataValidation: " ~ position ~ " needs a minimum, a maximum, or both") -%}
            {%- endif -%}
            {%- set min_sql = min_value if modules.re.match(number_re, min_value) else quote_text(min_value) -%}
            {%- set max_sql = max_value if modules.re.match(number_re, max_value) else quote_text(max_value) -%}
            {%- for q in quoted -%}
                {%- set bounds = [] -%}
                {%- if min_value != '' -%}{%- do bounds.append(q ~ ' >= ' ~ min_sql) -%}{%- endif -%}
                {%- if max_value != '' -%}{%- do bounds.append(q ~ ' <= ' ~ max_sql) -%}{%- endif -%}
                {%- do parts.append('(' ~ q ~ ' is null or (' ~ (bounds | join(' and ')) ~ '))') -%}
            {%- endfor -%}
            {%- set check_sql = parts | join(' and ') -%}

        {%- elif check_type == 'regex_match' -%}
            {%- set pattern = (rule.get('pattern') or '') | string -%}
            {%- if pattern == '' -%}
                {%- do exceptions.raise_compiler_error("DataValidation: " ~ position ~ " needs a pattern") -%}
            {%- endif -%}
            {%- for q in quoted -%}
                {%- do parts.append('(' ~ q ~ ' is null or ' ~ full_match(to_text(q), pattern) ~ ')') -%}
            {%- endfor -%}
            {%- set check_sql = parts | join(' and ') -%}

        {%- elif check_type == 'expression' -%}
            {%- set expression = (rule.get('expression') or '') | string | trim -%}
            {%- if expression == '' -%}
                {%- do exceptions.raise_compiler_error("DataValidation: " ~ position ~ " needs a SQL condition") -%}
            {%- endif -%}
            {%- set check_sql = expression -%}

        {%- elif check_type == 'required_columns' -%}
            {%- if schema is none or schema in ['', '[]'] -%}
                {%- do exceptions.raise_compiler_error("DataValidation: the required columns check needs the input schema") -%}
            {%- endif -%}
            {%- set schema_names = [] -%}
            {%- for f in (fromjson(schema) if schema is string else schema) -%}
                {%- do schema_names.append(bare_name(f.name) | lower) -%}
            {%- endfor -%}
            {%- set missing = [] -%}
            {%- for col in columns -%}
                {%- if bare_name(col) | lower not in schema_names -%}
                    {%- do missing.append(col) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if missing and on_failure == 'fail' -%}
                {%- do exceptions.raise_compiler_error("DataValidation failed: the input is missing required column(s): " ~ (missing | join(', '))) -%}
            {%- endif -%}
            {%- set check_sql = 'false' if missing else 'true' -%}
            {%- if missing -%}
                {%- set name_suffix.text = ' (missing: ' ~ (missing | join(', ')) ~ ')' -%}
            {%- endif -%}

        {%- elif check_type == 'row_count' -%}
            {%- if min_value == '' and max_value == '' -%}
                {%- do exceptions.raise_compiler_error("DataValidation: " ~ position ~ " needs a minimum, a maximum, or both") -%}
            {%- endif -%}
            {%- for bound in [min_value, max_value] -%}
                {%- if bound != '' and not modules.re.match('^[0-9]+$', bound) -%}
                    {%- do exceptions.raise_compiler_error("DataValidation: " ~ position ~ " bounds must be whole numbers, got: " ~ bound) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- set row_bounds = [] -%}
            {%- set guard_bounds = [] -%}
            {%- if min_value != '' -%}
                {%- do row_bounds.append('count(*) over () >= ' ~ min_value) -%}
                {%- do guard_bounds.append('count(*) >= ' ~ min_value) -%}
            {%- endif -%}
            {%- if max_value != '' -%}
                {%- do row_bounds.append('count(*) over () <= ' ~ max_value) -%}
                {%- do guard_bounds.append('count(*) <= ' ~ max_value) -%}
            {%- endif -%}
            {%- set check_sql = row_bounds | join(' and ') -%}
            {%- do guards.append({'index': loop.index, 'sql': guard_bounds | join(' and ')}) -%}
        {%- endif -%}

        {%- set custom_name = (rule.get('checkName') or '') | string | trim -%}
        {%- if custom_name != '' -%}
            {%- set check_name = custom_name -%}
        {%- elif columns | length > 0 and check_type != 'row_count' -%}
            {%- set check_name = type_labels[check_type] ~ ': ' ~ (columns | join(', ')) -%}
        {%- else -%}
            {%- set check_name = type_labels[check_type] -%}
        {%- endif -%}
        {%- do checks.append({
            'column': '_dq_check_' ~ loop.index,
            'sql': check_sql,
            'name': check_name ~ name_suffix.text,
            'type': check_type
        }) -%}
    {%- endfor -%}

    {%- set check_columns = [] -%}
    {%- set failed_parts = [] -%}
    {%- for c in checks -%}
        {%- do check_columns.append(c.column) -%}
        {%- do failed_parts.append("case when " ~ c.column ~ " then '' else " ~ quote_text('; ' ~ c.name) ~ " end") -%}
    {%- endfor -%}
    {%- set passed_sql = '(' ~ (check_columns | join(' and ')) ~ ')' -%}
    {%- set internal_columns = check_columns + ['_dq_passed', '_dq_failed_checks'] -%}
    {%- set input_columns = star_except(internal_columns) | trim -%}

    {%- set guard_conditions = [] -%}
    {%- for g in guards -%}
        {%- do guard_conditions.append(g.sql) -%}
    {%- endfor %}

with dq_flagged as (
    select
        *,
        {%- for c in checks %}
        coalesce(({{ c.sql }}), false) as {{ c.column }}{{ ',' if not loop.last }}
        {%- endfor %}
    from {{ source_table }}
),

dq_evaluated as (
    select
        *,
        {{ passed_sql }} as _dq_passed,
        nullif(substr({{ failed_parts | join(' || ') }}, 3), '') as _dq_failed_checks
    from dq_flagged
),

{%- if on_failure == 'fail' %}

dq_guard as (
    select {{ raise_if('_dq_error') }} as _dq_ok
    from (
        select
            case
                {%- if guard_conditions %}
                when not ({{ guard_conditions | join(' and ') }})
                    then 'DataValidation failed: the input has ' || {{ to_text('count(*)') }} || ' rows, which is outside the allowed row count'
                {%- endif %}
                when sum(case when _dq_passed then 0 else 1 end) > 0
                    then 'DataValidation failed: ' || {{ to_text('sum(case when _dq_passed then 0 else 1 end)') }} || ' row(s) failed at least one check'
            end as _dq_error
        from dq_evaluated
    ) dq_messages
),

dq_result as (
    select * from dq_evaluated
    where (select _dq_ok from dq_guard) = 1
)
{%- else %}

dq_result as (
    select * from dq_evaluated
)
{%- endif %}

{% if output_mode == 'summary' -%}
{%- for c in checks %}
select
    {{ loop.index }} as check_number,
    {{ quote_text(c.name) }} as check_name,
    '{{ c.type }}' as check_type,
    coalesce(sum(case when {{ c.column }} then 0 else 1 end), 0) as failed_rows,
    {%- set guard_sql = (guards | selectattr('index', 'equalto', loop.index) | map(attribute='sql') | list) %}
    {%- if guard_sql %}
    ({{ guard_sql[0] }}) as check_passed
    {%- else %}
    coalesce(sum(case when {{ c.column }} then 0 else 1 end), 0) = 0 as check_passed
    {%- endif %}
from dq_result
{%- if not loop.last %}
union all
{%- endif %}
{%- endfor %}
{%- elif output_mode == 'passed' -%}
select {{ input_columns }}
from dq_result
where _dq_passed
{%- elif output_mode == 'failed' -%}
select {{ input_columns }}, _dq_failed_checks as dq_failed_checks
from dq_result
where not _dq_passed
{%- else -%}
select {{ input_columns }}, _dq_passed as dq_passed, _dq_failed_checks as dq_failed_checks
from dq_result
{%- endif %}
{% endmacro %}


{# ---------- helpers: the only per-adapter differences ---------- #}

{% macro default__data_validation_to_text(expr) -%}
    {{ return('cast(' ~ expr ~ ' as string)') }}
{%- endmacro %}

{% macro duckdb__data_validation_to_text(expr) -%}
    {{ return('cast(' ~ expr ~ ' as varchar)') }}
{%- endmacro %}

{% macro snowflake__data_validation_to_text(expr) -%}
    {{ return('to_varchar(' ~ expr ~ ')') }}
{%- endmacro %}


{# Matches when the whole text matches the pattern, not just part of it. #}
{% macro default__data_validation_full_match(text_expr, pattern) -%}
    {{ return(text_expr ~ ' rlike ' ~ adapter.dispatch('data_validation_quote', 'prophecy_basics')('^(?:' ~ pattern ~ ')$')) }}
{%- endmacro %}

{% macro duckdb__data_validation_full_match(text_expr, pattern) -%}
    {{ return('regexp_full_match(' ~ text_expr ~ ', ' ~ adapter.dispatch('data_validation_quote', 'prophecy_basics')(pattern) ~ ')') }}
{%- endmacro %}

{% macro snowflake__data_validation_full_match(text_expr, pattern) -%}
    {{ return('regexp_like(' ~ text_expr ~ ', ' ~ adapter.dispatch('data_validation_quote', 'prophecy_basics')(pattern) ~ ')') }}
{%- endmacro %}

{% macro bigquery__data_validation_full_match(text_expr, pattern) -%}
    {{ return('regexp_contains(' ~ text_expr ~ ', ' ~ adapter.dispatch('data_validation_quote', 'prophecy_basics')('^(?:' ~ pattern ~ ')$') ~ ')') }}
{%- endmacro %}


{# A SQL string literal for text. DuckDB strings have no backslash escapes, so backslashes stay single there. #}
{% macro default__data_validation_quote(text) -%}
    {{ return("'" ~ (text | string | replace('\\', '\\\\') | replace("'", "''")) ~ "'") }}
{%- endmacro %}

{% macro duckdb__data_validation_quote(text) -%}
    {{ return("'" ~ (text | string | replace("'", "''")) ~ "'") }}
{%- endmacro %}


{# A column name without surrounding backticks or double quotes. #}
{% macro default__data_validation_bare_name(name) -%}
    {%- set name = name | string | trim -%}
    {%- if name | length > 1 and name[0] in ['`', '"'] and name[-1] == name[0] -%}
        {%- set name = name[1:-1] -%}
    {%- endif -%}
    {{ return(name) }}
{%- endmacro %}

{% macro default__data_validation_quote_column(name) -%}
    {{ return('`' ~ adapter.dispatch('data_validation_bare_name', 'prophecy_basics')(name) ~ '`') }}
{%- endmacro %}

{% macro duckdb__data_validation_quote_column(name) -%}
    {{ return('"' ~ (adapter.dispatch('data_validation_bare_name', 'prophecy_basics')(name) | replace('"', '""')) ~ '"') }}
{%- endmacro %}

{% macro snowflake__data_validation_quote_column(name) -%}
    {{ return('"' ~ (adapter.dispatch('data_validation_bare_name', 'prophecy_basics')(name) | replace('"', '""')) ~ '"') }}
{%- endmacro %}


{# 1 when message_expr is NULL; otherwise stops the query with message_expr as the error text. #}
{% macro default__data_validation_raise_if(message_expr) -%}
    {{ return('case when ' ~ message_expr ~ ' is null then 1 else raise_error(' ~ message_expr ~ ') end') }}
{%- endmacro %}

{% macro duckdb__data_validation_raise_if(message_expr) -%}
    {{ return('case when ' ~ message_expr ~ ' is null then 1 else error(' ~ message_expr ~ ') end') }}
{%- endmacro %}

{% macro bigquery__data_validation_raise_if(message_expr) -%}
    {{ return('case when ' ~ message_expr ~ ' is null then 1 else error(' ~ message_expr ~ ') end') }}
{%- endmacro %}

{# Snowflake has no raise function: casting the message text to a number fails and shows the message. #}
{% macro snowflake__data_validation_raise_if(message_expr) -%}
    {{ return("cast(coalesce(" ~ message_expr ~ ", '1') as int)") }}
{%- endmacro %}


{% macro default__data_validation_star_except(columns) -%}
    {{ return('* except (' ~ (columns | join(', ')) ~ ')') }}
{%- endmacro %}

{% macro duckdb__data_validation_star_except(columns) -%}
    {{ return('* exclude (' ~ (columns | join(', ')) ~ ')') }}
{%- endmacro %}

{% macro snowflake__data_validation_star_except(columns) -%}
    {{ return('* exclude (' ~ (columns | join(', ')) ~ ')') }}
{%- endmacro %}
