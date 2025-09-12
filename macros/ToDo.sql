{% macro ToDo(diag_message) -%}
    {{ return(adapter.dispatch('ToDo', 'prophecy_basics')(diag_message)) }}
{% endmacro %}


{% macro databricks__ToDo(diag_message) %}
    SELECT *
    FROM (
        SELECT cast(1 as string) as error_message
    ) AS dummy
    WHERE raise_error('ToDo: {{ diag_message }}') IS NULL
{% endmacro %}

{% macro duckdb__ToDo(diag_message) %}
    {# DuckDB implementation of ToDo macro with error handling #}
    CASE 
        WHEN 1=1 THEN error('ToDo: {{ diag_message }}')
        ELSE 1
    END
{% endmacro %}