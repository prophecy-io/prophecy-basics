{#
  ToDo Macro Gem
  ==============

  Placeholder macro that fails at runtime with a diagnostic message (adapter-specific).

  Parameters:
    - diag_message (string): Message embedded in the raised error.

  Adapter Support:
    - default__ (raise_error), duckdb__ (error), bigquery__ (ERROR), snowflake__ (invalid cast)

  Macro Call Examples:
    {{ prophecy_basics.ToDo('Replace this model with real logic') }}
#}
{% macro ToDo(diag_message) -%}
    {{ return(adapter.dispatch('ToDo', 'prophecy_basics')(diag_message)) }}
{% endmacro %}


{% macro default__ToDo(diag_message) %}
    SELECT *
    FROM (
        SELECT cast(1 as string) as error_message
    ) AS dummy
    WHERE raise_error('ToDo: {{ diag_message }}') IS NULL
{% endmacro %}

{% macro duckdb__ToDo(diag_message) %}
    SELECT *
    FROM (
        SELECT CAST(1 AS VARCHAR) AS error_message
    ) AS dummy
    WHERE error('ToDo: {{ diag_message }}') IS NULL
{% endmacro %}

{% macro bigquery__ToDo(diag_message) %}
    SELECT ERROR(CONCAT('ToDo: ', '{{ diag_message }}'))
{% endmacro %}

{% macro snowflake__ToDo(diag_message) %}
    SELECT CAST('ToDo: {{ diag_message }}' AS INT) AS error_message
{% endmacro %}