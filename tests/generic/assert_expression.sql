{% test assert_expression(model, expression, column_name=None) %}

{% set column_list = '*' if should_store_failures() else "1" %}

select
    {{ column_list }}
from {{ model }}
where not(COALESCE({{ expression }}, FALSE))

{% endtest %}