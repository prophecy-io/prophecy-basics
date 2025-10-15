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

