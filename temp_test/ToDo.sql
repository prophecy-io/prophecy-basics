{% macro ToDo(diag_message) -%}
    {{ return(adapter.dispatch('ToDo', 'prophecy_basics')(diag_message)) }}
{% endmacro %}


{% macro databricks__ToDo(diag_message) %}
    SELECT *
    FROM (
        SELECT cast(1 as string) as error_message
    ) AS dummy
    WHERE raise_error('ToDo: {{ diag_message }}') IS NULL

{%- macro duckdb__ToDo(diag_message) -%}
    {# Basic ToDo implementation for DuckDB #}
    SELECT 1 as dummy
{%- endmacro -%}
{% endmacro %}