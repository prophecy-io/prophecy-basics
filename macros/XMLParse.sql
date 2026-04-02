{#
  XMLParse Macro Gem
  ==================

  Reads XML stored in a text column and expands it into a structured column you
  can query, using a schema or a sample document—or leaves the table unchanged
  when parsing is disabled.

  Parameters:
    - relation_name (list): Source relation(s).
    - columnName: Column to parse; empty → SELECT *.
    - parsingMethod: 'parseFromSchema' | 'parseFromSampleRecord' | 'none' / empty.
    - sampleRecord, sampleSchema: XML sample or schema string for Spark from_xml.

  Adapter Support:
    - default__ (from_xml), snowflake__ (PARSE_XML), duckdb__ (pass-through SELECT *)

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {{ prophecy_basics.XMLParse('t', 'xml_col', 'parseFromSchema', '', '<root>...</root>') }}
    {{ prophecy_basics.XMLParse('t', 'xml_col', 'none', '', '') }}

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.XMLParse('t', 'xml_col', 'parseFromSchema', '', '<root>...</root>') }}

    Resolved query (default__):
      select
          *,
          from_xml(`xml_col`, '<root>...</root>') as `xml_col_parsed`
      from t
#}
{% macro XMLParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema) -%}
    {{ return(adapter.dispatch('XMLParse', 'prophecy_basics')(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema)) }}
{% endmacro %}


{% macro default__XMLParse(
    relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema
) %}

    {{ log("Parsing XML using method: " ~ parsingMethod, info=True) }}
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
                from_xml({{ quoted_col }}, '{{ sampleSchema | replace("\n", " ") }}') as {{ alias_col }}
            from {{ relation_list | join(', ') }}

        {%- elif parsingMethod == 'parseFromSampleRecord' -%}
            select
                *,
                from_xml({{ quoted_col }}, schema_of_xml('{{ sampleRecord | replace("\n", " ") }}')) as {{ alias_col }}
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

{% macro snowflake__XMLParse(
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
            PARSE_XML({{ '"' ~ columnName ~ '"' }}) as {{ '"' ~ columnName ~ '_parsed"' }}
        from {{ relation_list | join(', ') }}
    {%- else -%}
        select * from {{ relation_list | join(', ') }}
    {%- endif -%}

{% endmacro %}

{%- macro duckdb__XMLParse(relation_name,
    columnName,
    parsingMethod,
    sampleRecord,
    sampleSchema) -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {# Simple XML parsing implementation for DuckDB - returns original data #}
    SELECT * FROM {{ relation_list | join(', ') }}
{%- endmacro -%}