{#
  JSONParse Macro Gem
  ===================

  Turns a JSON text column into structured columns (nested struct) using a schema
  you provide or infer from a sample record—or returns the table unchanged when
  parsing is turned off.

  Parameters:
    - relation_name (string or list): Source relation(s).
    - columnName: Column to parse; empty → SELECT *.
    - parsingMethod: 'parseFromSchema' (needs sampleSchema), 'parseFromSampleRecord' (needs sampleRecord),
        'none' or empty → pass-through; other values → compiler error.
    - sampleRecord, sampleSchema: DDL/sample JSON string for Spark from_json.

  Adapter Support:
    - default__ (from_json, backticks), snowflake__ (PARSE_JSON), duckdb__ (json_extract)

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {{ prophecy_basics.JSONParse('t', 'payload', 'parseFromSchema', '', '{"type":"struct","fields":[...]}') }}
    {{ prophecy_basics.JSONParse('t', 'payload', 'parseFromSampleRecord', '{"a":1}', '') }}
    {{ prophecy_basics.JSONParse('t', '', 'none', '', '') }}

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.JSONParse('t', 'payload', 'parseFromSchema', '', '{"type":"struct","fields":[...]}') }}

    Resolved query (default__ — schema string shortened):
      select
          *,
          from_json(`payload`, '{"type":"struct","fields":[...]}') as `payload_parsed`
      from t
#}
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

{% macro snowflake__JSONParse(
    relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema
    ) %}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if columnName -%}
        select
            *,
            PARSE_JSON({{ '"' ~ columnName ~ '"' }}) as {{ '"' ~ columnName ~ '_parsed"' }}
        from {{ relation_list | join(', ') }}
    {%- else -%}
        select * from {{ relation_list | join(', ') }}
    {%- endif -%}
    
{% endmacro %}

{%- macro duckdb__JSONParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema) -%}

    {{ log("Parsing JSON using method: " ~ parsingMethod, info=True) }}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if not columnName or columnName | trim == '' -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseFromSchema' and (not sampleSchema or sampleSchema | trim == '') -%}
        select * from {{ relation_list | join(', ') }}

    {%- elif parsingMethod == 'parseFromSampleRecord' and (not sampleRecord or sampleRecord | trim == '') -%}
        select * from {{ relation_list | join(', ') }}

    {%- else -%}
        {%- set quoted_col = prophecy_basics.quote_identifier(columnName) -%}
        {%- set alias_col = prophecy_basics.quote_identifier(columnName ~ '_parsed') -%}

        {%- if parsingMethod == 'parseFromSchema' -%}
            select
                *,
                json_extract({{ quoted_col }}, '$') as {{ alias_col }}
            from {{ relation_list | join(', ') }}

        {%- elif parsingMethod == 'parseFromSampleRecord' -%}
            select
                *,
                json_extract({{ quoted_col }}, '$') as {{ alias_col }}
            from {{ relation_list | join(', ') }}

        {%- elif parsingMethod == 'none' or not parsingMethod -%}
            select * from {{ relation_list | join(', ') }}

        {%- else -%}
            {{ exceptions.raise_compiler_error(
                "Invalid parsingMethod: '" ~ parsingMethod ~ "'. Expected 'parseFromSchema', 'parseFromSampleRecord', or 'none'."
            ) }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro -%}