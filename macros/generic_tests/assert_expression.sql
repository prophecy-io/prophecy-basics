{% test assert_expression(model, expression, column_name=None) %}
  {{ return(adapter.dispatch('test_assert_expression', 'prophecy_basics')(model, expression)) }}
{% endtest %}

{% macro default__test_assert_expression(model, expression) %}

{% set column_list = '*' if should_store_failures() else "1" %}

select
    {{ column_list }}
from {{ model }}
where not(COALESCE({{ expression }}, FALSE))

{% endmacro %}