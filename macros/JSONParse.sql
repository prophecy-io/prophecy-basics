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
        {%- set quoted_col = "`" ~ columnName ~ "`" -%}
        {%- set alias_col = "`" ~ columnName ~ "_parsed`" -%}

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

{%- macro duckdb__JSONParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema) -%}

    {{ log("DuckDB: Parsing JSON column with method: " ~ parsingMethod, info=True) }}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if not columnName or columnName | trim == '' -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseFromSchema' and sampleSchema and sampleSchema | trim != '' -%}
        {%- set quoted_col = All_Provider_Gems_Test_2.quote_identifier(columnName) -%}
        {%- set alias_col = All_Provider_Gems_Test_2.quote_identifier(columnName ~ '_parsed') -%}

        WITH parsed_data AS (
            SELECT 
                *,
                CAST({{ quoted_col }} AS {{ sampleSchema }}) AS {{ alias_col }}
            FROM {{ relation_list | join(', ') }}
        )
        SELECT * FROM parsed_data
    {%- else -%}
        select * from {{ relation_list | join(', ') }}
    {%- endif -%}

{%- endmacro -%}