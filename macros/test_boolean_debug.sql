{% macro test_boolean_debug(leaveExtraCharLastCol) %}
  {% if leaveExtraCharLastCol == 'Leave extra in last column' %}
    {% set leaveExtraCharLastCol = true %}
  {% else %}
    {% set leaveExtraCharLastCol = false %}
  {% endif %}
  
  {% if leaveExtraCharLastCol %}
    SELECT 'TRUE' as result
  {% else %}
    SELECT 'FALSE' as result
  {% endif %}
{% endmacro %}

{{ test_boolean_debug('Leave extra in last column') }}
