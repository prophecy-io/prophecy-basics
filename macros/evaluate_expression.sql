{% macro evaluate_expression(expression, column='') -%}
    {{ return(adapter.dispatch('evaluate_expression', 'prophecy_basics')(expression, column)) }}
{%- endmacro %}

{% macro databricks__evaluate_expression(expression, column='') %}
{#
  Evaluate a SQL expression and return the result for Databricks.
  
  Args:
    expression: SQL expression to evaluate
    column: Fallback value (optional, for compile-time compatibility)
  
  Returns:
    String result of the expression evaluation
#}
{% if execute %}
  {% set sql_query = 'SELECT ' ~ expression ~ ' AS result' %}
  {% set result = run_query(sql_query) %}
  {% if result and result.rows %}
    {{ result.rows[0][0] }}
  {% else %}
    {{ column if column else 'false' }}
  {% endif %}
{% else %}
  {# Fallback for compile time #}
  {{ column if column else 'false' }}
{% endif %}
{% endmacro %}

{% macro snowflake__evaluate_expression(expression,column) %}
{% set sql_query = 'select ' ~ expression ~ ' as result' %}
{% set result = run_query(sql_query) %}
{% if result %}
  {% if execute %}
    {% for row in result.rows %}
      {{row[0]}}
    {% endfor %}
  {% endif %}
{% else %}
  {# Added to fake dbt query at compile time #}
  {{ column }}
{% endif %}
{% endmacro %}