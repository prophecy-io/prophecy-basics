{% macro TextToColumns(relation_name,
    columnName,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName   ) -%}
    {{ return(adapter.dispatch('TextToColumns', 'prophecy_basics')(relation_name,
    columnName,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName   )) }}
{% endmacro %}


{% macro bigquery__TextToColumns(
    relation_name,
    columnName,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName    
    ) %}

{# 
  Build the regex pattern for matching the delimiter.
#}
{%- set pattern = delimiter -%}

{# Helper to quote column names inline #}
{%- set quote_char = '`' -%}

{# Quote the column name properly #}
{%- set quoted_column_name = prophecy_basics.quote_identifier(columnName) -%}

{%- if split_strategy == 'splitColumns' -%}
    WITH source AS (
        SELECT *,
            SPLIT(
                REGEXP_REPLACE(`{{ quoted_column_name }}`, {{ "'" ~ pattern ~ "'" }}, '%%DELIM%%'),
                '%%DELIM%%'
            ) AS tokens
        FROM {{ relation_name }}
    ),
    all_data AS (
    SELECT *,
        {# Extract tokens positionally (BigQuery arrays are 0-indexed) #}
        {%- for i in range(1, noOfColumns) %}
            CASE
                WHEN ARRAY_LENGTH(tokens) > {{ i - 1 }}
                    THEN REGEXP_REPLACE(TRIM(tokens[OFFSET({{ i - 1 }})]), r'^"|"$', '')
                ELSE null
            END AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ i ~ '_' ~ splitColumnSuffix ~ quote_char }}{% if not loop.last or leaveExtraCharLastCol %}, {% endif %}
        {%- endfor %}
        {%- if leaveExtraCharLastCol %}
            CASE
                WHEN ARRAY_LENGTH(tokens) >= {{ noOfColumns }}
                    THEN ARRAY_TO_STRING(ARRAY(SELECT tokens[OFFSET(i)] FROM UNNEST(GENERATE_ARRAY({{ noOfColumns - 1 }}, ARRAY_LENGTH(tokens) - 1)) AS i), '{{ delimiter }}')
                ELSE null
            END AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix ~ quote_char }}
        {%- else %}
            CASE
                WHEN ARRAY_LENGTH(tokens) > {{ noOfColumns - 1 }}
                    THEN REGEXP_REPLACE(TRIM(tokens[OFFSET({{ noOfColumns - 1 }})]), r'^"|"$', '')
                ELSE null
            END AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix ~ quote_char }}
        {%- endif %}
    FROM source
    )
    SELECT * EXCEPT(tokens) FROM all_data

{%- elif split_strategy == 'splitRows' -%}
    SELECT r.*,
        REGEXP_REPLACE(TRIM(split_value), r'[{}_]', ' ') AS {{ quote_char ~ splitRowsColumnName ~ quote_char }}
    FROM {{ relation_name }} r,
    UNNEST(SPLIT(COALESCE(r.`{{ quoted_column_name }}`, ''), '{{ pattern }}')) AS split_value

{%- else -%}
SELECT * FROM {{ relation_name }}
{%- endif -%}

{% endmacro %}


{% macro databricks__TextToColumns(
    relation_name,
    columnName,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName    
    ) %}

{# 
  Build the regex pattern for matching the delimiter.
#}
{%- set pattern = delimiter -%}

{# Helper to quote column names inline #}
{%- set quote_char = '`' -%}

{# Quote the column name properly #}
{%- set quoted_column_name = prophecy_basics.quote_identifier(columnName) -%}

{%- if split_strategy == 'splitColumns' -%}
    WITH source AS (
        SELECT *,
            split(
                regexp_replace({{ quoted_column_name }}, {{ "'" ~ pattern ~ "'" }}, '%%DELIM%%'),
                '%%DELIM%%'
            ) AS tokens
        FROM {{ relation_name }}
    ),
    all_data AS (
    SELECT *,
        {# Extract tokens positionally (Spark arrays are 0-indexed) #}
        {%- for i in range(1, noOfColumns) %}
            regexp_replace(trim(tokens[{{ i - 1 }}]), '^"|"$', '')
            AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ i ~ '_' ~ splitColumnSuffix ~ quote_char }}{% if not loop.last or leaveExtraCharLastCol %}, {% endif %}
        {%- endfor %}
        {%- if leaveExtraCharLastCol %}
            CASE
                WHEN size(tokens) >= {{ noOfColumns }}
                    THEN array_join(slice(tokens, {{ noOfColumns }}, greatest(size(tokens) - {{ noOfColumns }} + 1, 0)), '{{ delimiter }}')
                ELSE null
            END AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix ~ quote_char }}
        {%- else %}
            tokens[{{ noOfColumns - 1 }}] AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix ~ quote_char }}
        {%- endif %}
    FROM source
    )
    SELECT * EXCEPT(tokens) FROM all_data

{%- elif split_strategy == 'splitRows' -%}
    SELECT r.*,
        trim(regexp_replace(s.col, '[{}_]', ' ')) AS {{ quote_char ~ splitRowsColumnName ~ quote_char }}
    FROM {{ relation_name }} r
    LATERAL VIEW explode(
        split(
            if({{ quoted_column_name }} IS NULL, '', {{ quoted_column_name }}),
            '{{ pattern }}'
        )
    ) s AS col

{%- else -%}
SELECT * FROM {{ relation_name }}
{%- endif -%}

{% endmacro %}