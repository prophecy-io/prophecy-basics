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

{%- endmacro -%}

{%- macro duckdb__TextToColumns(
    relation_name,
    columnName,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName
) -%}

{# Handle escaped delimiters from Python - remove backslashes entirely #}
{%- set pattern = delimiter.replace('\\', '') -%}
{# Handle string boolean parameter from Python #}
{%- set leaveExtraCharLastCol = leaveExtraCharLastCol == 'Leave extra in last column' -%}
{%- set quoted_column = prophecy_basics.quote_identifier(columnName) -%}

{%- if split_strategy == 'splitColumns' -%}
  WITH r AS (
    SELECT * FROM {{ relation_name }}
  )
  SELECT
    r.*,
    {# First N-1 split columns #}
    {%- for i in range(1, noOfColumns) %}
      regexp_replace(
        trim( (string_split(r.{{ quoted_column }}, '{{ pattern }}'))[{{ i }}] ),
        '^"|"$',
        '',
        'g'
      ) AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ i ~ '_' ~ splitColumnSuffix) }}{% if not loop.last or leaveExtraCharLastCol %},{% endif %}
    {%- endfor %}
    {%- if leaveExtraCharLastCol %}
      {# Special case: if noOfColumns=1, return the entire original string #}
      {%- if noOfColumns == 1 %}
      r.{{ quoted_column }} AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix) }}
      {%- else %}
      CASE
        WHEN (string_split(r.{{ quoted_column }}, '{{ pattern }}'))[{{ noOfColumns }}] IS NOT NULL
          THEN array_to_string(
                 (string_split(r.{{ quoted_column }}, '{{ pattern }}'))[{{ noOfColumns }}:],
                 '{{ delimiter }}'
               )
        ELSE NULL
      END AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix) }}
      {%- endif %}
    {%- else %}{% if noOfColumns > 1 %},{% endif %}
      (string_split(r.{{ quoted_column }}, '{{ pattern }}'))[{{ noOfColumns }}]
        AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix) }}
    {%- endif %}
  FROM r

{%- elif split_strategy == 'splitRows' -%}
  SELECT
    r.*,
    -- replace { } _ globally with spaces, collapse repeats, then trim
    trim(
      regexp_replace(
        regexp_replace(s.col, '[{}_]', ' ', 'g'),
        '\s+',
        ' ',
        'g'
      )
    ) AS {{ prophecy_basics.quote_identifier(splitRowsColumnName) }}
  FROM {{ relation_name }} r
  CROSS JOIN UNNEST(
    string_split(coalesce(r.{{ quoted_column }}, ''), '{{ pattern }}')
  ) AS s(col)

{%- else -%}
  SELECT * FROM {{ relation_name }}
{%- endif -%}

{%- endmacro -%}