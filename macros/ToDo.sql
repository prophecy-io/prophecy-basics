{#
  ToDo Macro Gem
  ==============

  Marks unfinished work in a model: when this runs, the query stops with a clear
  message so you do not silently ship placeholder logic to production.

  Parameters:
    - diag_message (string): Message embedded in the raised error.

  Adapter Support:
    - default__ (raise_error), duckdb__ (error), bigquery__ (ERROR), snowflake__ (invalid cast)

  Depends on schema parameter:
    No

  Macro Call Examples:
    {{ prophecy_basics.ToDo('Replace this model with real logic') }}

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.ToDo('Replace this model with real logic') }}

    Resolved query (default__):
      SELECT *
      FROM (
          SELECT cast(1 as string) as error_message
      ) AS dummy
      WHERE raise_error('ToDo: Replace this model with real logic') IS NULL
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