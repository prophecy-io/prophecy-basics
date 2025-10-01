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