{% macro JSONParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema) -%}
    {{ return(adapter.dispatch('JSONParse', 'prophecy_basics')(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema)) }}
{% endmacro %}


{% macro default__JSONParse(
    relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema
) %}

    {{ log("Parsing JSON using method: " ~ parsingMethod, info=True) }}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if not columnName or columnName | trim == '' -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseFromSchema' and (not sampleSchema or sampleSchema | trim == '') -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseFromSampleRecord' and (not sampleRecord or sampleRecord | trim == '') -%}
        select * from {{ relation_list | join(', ') }}

    {%- else -%}
        {%- set quoted_col = prophecy_basics.quote_identifier(columnName) | trim -%}
        {%- set alias_col = prophecy_basics.quote_identifier(columnName ~ '_parsed') | trim -%}

        {%- if parsingMethod == 'parseFromSchema' -%}
            select
                *,
                from_json({{ quoted_col }}, '{{ sampleSchema | replace("\n", " ") }}') as {{ alias_col }}
            from {{ relation_list | join(', ') }}

        {%- elif parsingMethod == 'parseFromSampleRecord' -%}
            select
                *,
                from_json({{ quoted_col }}, schema_of_json('{{ sampleRecord | replace("\n", " ") }}')) as {{ alias_col }}
            from {{ relation_list | join(', ') }}

        {%- elif parsingMethod == 'none' or not parsingMethod -%}
            select * from {{ relation_list | join(', ') }}

        {%- else -%}
            {{ exceptions.raise_compiler_error(
                "Invalid parsingMethod: '" ~ parsingMethod ~ "'. Expected 'parseFromSchema', 'parseFromSampleRecord', or 'none'."
            ) }}
        {%- endif -%}
    {%- endif -%}

{% endmacro %}

{# DuckDB: Simple JSON parsing using json_extract #}
{%- macro duckdb__JSONParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema) -%}

    {{ log("Parsing JSON (DuckDB)", info=True) }}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if not columnName or columnName | trim == '' -%}
        select * from {{ relation_list | join(', ') }}
    {%- else -%}
        {%- set quoted_col = prophecy_basics.quote_identifier(columnName) | trim -%}
        {%- set alias_col = prophecy_basics.quote_identifier(columnName ~ '_parsed') | trim -%}
        select
            *,
            json_extract({{ quoted_col }}, '$') as {{ alias_col }}
        from {{ relation_list | join(', ') }}
    {%- endif -%}

{%- endmacro -%}

{# BigQuery: Simple JSON parsing using PARSE_JSON #}
{%- macro bigquery__JSONParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema) -%}

    {{ log("Parsing JSON (BigQuery)", info=True) }}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if not columnName or columnName | trim == '' -%}
        select * from {{ relation_list | join(', ') }}
    {%- else -%}
        {%- set quoted_col = prophecy_basics.quote_identifier(columnName) | trim -%}
        {%- set alias_col = prophecy_basics.quote_identifier(columnName ~ '_parsed') | trim -%}
        select
            *,
            PARSE_JSON({{ quoted_col }}) as {{ alias_col }}
        from {{ relation_list | join(', ') }}
    {%- endif -%}

{%- endmacro -%}