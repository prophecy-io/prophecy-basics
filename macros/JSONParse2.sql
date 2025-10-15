{% macro default__JSONParse(
    relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema
) %}

    {{ log("Parsing JSON using method: " ~ parsingMethod, info=True) }}

    {%- if not columnName or columnName | trim == '' -%}
        select * from {{ relation_name }}

    {%- elif parsingMethod == 'parseFromSchema' and (not sampleSchema or sampleSchema | trim == '') -%}
        select * from {{ relation_name }}

    {%- elif parsingMethod == 'parseFromSampleRecord' and (not sampleRecord or sampleRecord | trim == '') -%}
        select * from {{ relation_name }}

    {%- else -%}
        {%- set quoted_col = adapter.quote(columnName) -%}
        {%- set alias_col = adapter.quote(columnName ~ '_parsed') -%}

        {%- if parsingMethod == 'parseFromSchema' -%}
            select
                *,
                from_json({{ quoted_col }}, '{{ sampleSchema | replace("\n", " ") }}') as {{ alias_col }}
            from {{ relation_name }}

        {%- elif parsingMethod == 'parseFromSampleRecord' -%}
            select
                *,
                from_json({{ quoted_col }}, schema_of_json('{{ sampleRecord | replace("\n", " ") }}')) as {{ alias_col }}
            from {{ relation_name }}

        {%- elif parsingMethod == 'none' or not parsingMethod -%}
            select * from {{ relation_name }}

        {%- else -%}
            {{ exceptions.raise_compiler_error(
                "Invalid parsingMethod: '" ~ parsingMethod ~ "'. Expected 'parseFromSchema', 'parseFromSampleRecord', or 'none'."
            ) }}
        {%- endif -%}
    {%- endif -%}

{% endmacro %}

{%- macro duckdb__JSONParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema) -%}

    {{ log("Parsing JSON using method: " ~ parsingMethod, info=True) }}

    {%- if not columnName or columnName | trim == '' -%}
        select * from {{ relation_name }}

    {%- elif parsingMethod == 'parseFromSchema' and (not sampleSchema or sampleSchema | trim == '') -%}
        select * from {{ relation_name }}

    {%- elif parsingMethod == 'parseFromSampleRecord' and (not sampleRecord or sampleRecord | trim == '') -%}
        select * from {{ relation_name }}

    {%- else -%}
        {%- set quoted_col = prophecy_basics.quote_identifier(columnName) -%}
        {%- set alias_col = prophecy_basics.quote_identifier(columnName ~ '_parsed') -%}

        {%- if parsingMethod == 'parseFromSchema' -%}
            select
                *,
                json_extract({{ quoted_col }}, '$') as {{ alias_col }}
            from {{ relation_name }}

        {%- elif parsingMethod == 'parseFromSampleRecord' -%}
            select
                *,
                json_extract({{ quoted_col }}, '$') as {{ alias_col }}
            from {{ relation_name }}

        {%- elif parsingMethod == 'none' or not parsingMethod -%}
            select * from {{ relation_name }}

        {%- else -%}
            {{ exceptions.raise_compiler_error(
                "Invalid parsingMethod: '" ~ parsingMethod ~ "'. Expected 'parseFromSchema', 'parseFromSampleRecord', or 'none'."
            ) }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro -%}
