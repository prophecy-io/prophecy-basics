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
EXECUTE IMMEDIATE $$
DECLARE
    todo_exception EXCEPTION (-20001, 'ToDo: {{ diag_message }}');
BEGIN
    RAISE todo_exception;
EXCEPTION
    WHEN todo_exception THEN
        RETURN SQLERRM;
END;
$$;
{% endmacro %}