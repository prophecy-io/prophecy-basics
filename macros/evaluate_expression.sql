{% macro evaluate_expression(expression, column='') %}
{#
  Evaluate a SQL expression and return the result.
  
  Args:
    expression: SQL expression to evaluate
    column: Fallback value (optional, for compile-time compatibility)
  
  Returns:
    String result of the expression evaluation
#}
{% set sql_query = 'SELECT ' ~ expression ~ ' AS result' %}
{% set result = run_query(sql_query) %}
{% if result %}
  {% if execute %}
    {% for row in result.rows %}
      {{ row[0] }}
    {% endfor %}
  {% endif %}
{% else %}
  {# Fallback for compile time or when query fails #}
  {{ column if column else 'false' }}
{% endif %}
{% endmacro %}