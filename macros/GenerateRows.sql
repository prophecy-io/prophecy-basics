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
    max_rows='100000',
    force_mode='recursive'
) %}
{% if init_expr is none or init_expr == '' %}{% do exceptions.raise_compiler_error("init_expr is required") %}{% endif %}
{% if condition_expr is none or condition_expr == '' %}{% do exceptions.raise_compiler_error("condition_expr is required") %}{% endif %}
{% if loop_expr is none or loop_expr == '' %}{% do exceptions.raise_compiler_error("loop_expr is required") %}{% endif %}
{% if max_rows is none or max_rows == '' %}{% set max_rows = '100000' %}{% endif %}
{% set alias = "src" %}
{% set relation_tables = (relation_name if relation_name is iterable and relation_name is not string else [relation_name]) | join(', ')  %}
{% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}
{% set internal_col = "__gen_" ~ unquoted_col | replace(' ', '_') %}
{% set is_timestamp = " " in init_expr %}
{% set is_date = ("-" in init_expr) and not is_timestamp %}
{% set init_strip = init_expr.strip() %}
{% if init_strip.startswith("'") or init_strip.startswith('"') %}{% set init_value = init_strip %}{% else %}{% set init_value = "'" ~ init_strip ~ "'" %}{% endif %}
{% if is_timestamp %}{% set init_select = "to_timestamp(" ~ init_value ~ ")" %}{% elif is_date %}{% set init_select = "to_date(" ~ init_value ~ ")" %}{% else %}{% set init_select = init_expr %}{% endif %}
{% if '"' in condition_expr and "'" not in condition_expr %}{% set condition_expr_sql = condition_expr.replace('"', "'") %}{% else %}{% set condition_expr_sql = condition_expr %}{% endif %}
{% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
{% set backtick_col = "`" ~ unquoted_col ~ "`" %}
{% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
{% set singleq_col = "'" ~ unquoted_col ~ "'" %}
{% set plain_col = unquoted_col %}
{% set condition_expr_sql = replace_identifiers(condition_expr_sql, plain_col, internal_col, False) %}
{% set loop_expr_replaced_gen = replace_identifiers(loop_expr, plain_col, internal_col, True) %}
{% set loop_expr_replaced_plain = replace_identifiers(loop_expr, plain_col, internal_col, False) %}
{% set except_col = prophecy_basics.safe_identifier(unquoted_col) %}
{% set recursion_condition = condition_expr_sql | replace(internal_col, 'gen.' ~ internal_col) %}
{% set allowed = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
{% set specials = [] %}
{% for ch in unquoted_col %}{% if ch not in allowed %}{% do specials.append(ch) %}{% endif %}{% endfor %}
{% if specials | length > 0 %}{% set output_col_alias = prophecy_basics.quote_identifier(unquoted_col) %}{% else %}{% set output_col_alias = unquoted_col %}{% endif %}
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
        {{ loop_expr_replaced_gen }} as {{ internal_col }},
        _iter + 1
    from gen
    where _iter < {{ max_rows | int }}
      and ({{ recursion_condition }})
)
select
    payload.* EXCEPT ({{ except_col }}),
    {{ internal_col }} as {{ output_col_alias }}
from gen
where {{ condition_expr_sql }}
{% else %}
with recursive gen as (
    select {{ init_select }} as {{ internal_col }}, 1 as _iter
    union all
    select
        {{ loop_expr_replaced_plain }} as {{ internal_col }},
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