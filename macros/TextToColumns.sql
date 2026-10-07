{#
  TextToColumns Macro Gem
  =======================

  Splits one delimited text field into several new columns (fixed positions) or
  into many rows (one token per row)—useful for CSV-like blobs, tags, or multi-value
  fields stored in a single string.

  Parameters:
    - relation_name (list): Source relation(s).
    - columnNames: Single column name to split (string).
    - delimiter: REGEX pattern used to split. Plain text (',', ';', '\t') works as-is;
      regex is honored too (e.g. '[,]', '[|]', '\\s*,\\s*'). To match a regex
      metacharacter literally (. | ( ) [ ] { } ^ $ * + ? \), escape it or wrap it in
      a character class (e.g. '[|]' for a pipe, '[.]' for a dot).
    - split_strategy: 'splitColumns' | 'splitRows' | other → SELECT * pass-through.
    - noOfColumns: Number of output pieces for splitColumns.
    - leaveExtraCharLastCol: True / 'Leave extra in last column' — merge overflow into last column.
      (Overflow is rejoined using the delimiter text; lossless for plain delimiters, best-effort for regex.)
    - splitColumnPrefix, splitColumnSuffix: Name pattern prefix_i_suffix for split columns.
    - splitRowsColumnName: Output token column for splitRows.

  Behavior (aligned across all adapters):
    - The delimiter is always treated as a regex, implemented uniformly as
      REGEXP_REPLACE(col, <delimiter>, '%%DELIM%%') then split on the '%%DELIM%%' sentinel.
    - Split tokens are returned verbatim (no quote stripping / no character replacement).

  Adapter Support:
    - default__ (Spark/Databricks), bigquery__, snowflake__, duckdb__

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {{ prophecy_basics.TextToColumns(['t'], 'payload', ',', 'splitColumns', 4, False, 'c', 'out', 'token') }}
    {{ prophecy_basics.TextToColumns(['t'], 'payload', '[|]', 'splitRows', 1, False, 'in', 'out', 'part') }}

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.TextToColumns(['t'], 'payload', ',', 'splitColumns', 4, False, 'c', 'out', 'token') }}

    Resolved query (default__ — splitColumns; abbreviated column list):
      WITH source AS (
          SELECT *,
              SPLIT(
                  REGEXP_REPLACE(`payload`, ',', '%%DELIM%%'),
                  '%%DELIM%%'
              ) AS tokens
          FROM t
      ),
      all_data AS (
          SELECT *,
              tokens[0] AS `c_1_out`,
              tokens[1] AS `c_2_out`,
              tokens[2] AS `c_3_out`,
              tokens[3] AS `c_4_out`
          FROM source
      )
      SELECT * EXCEPT(tokens) FROM all_data
#}
{% macro TextToColumns(relation_name,
    columnNames,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName   ) -%}
    {{ return(adapter.dispatch('TextToColumns', 'prophecy_basics')(relation_name,
    columnNames,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName   )) }}
{% endmacro %}

{% macro default__TextToColumns(
    relation_name,
    columnNames,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName
    ) %}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

{# Helper to quote column names inline #}
{%- set quote_char = '`' -%}

{# Quote the column name properly #}
{%- set quoted_column_name = prophecy_basics.quote_identifier(columnNames) | trim -%}
{# The gem auto-detects literal vs regex delimiters: a literal like | or . arrives
   already re.escaped (\| , \.), while a regex like [,] or \d+ arrives as-is. Escape the
   backslashes (escape_backslashes=true) so Sparks string parser does NOT strip them and
   the regex engine receives exactly what the gem intended (\| -> literal pipe, \d -> digit
   class, [,] -> comma). The delimiter is used directly as the split regex below (no extra
   [] wrapping, which previously split on a leading [ and produced an empty first column). #}
{%- set delimiter_literal = prophecy_basics.escape_sql_string(delimiter, escape_backslashes=true) -%}

{%- if split_strategy == 'splitColumns' -%}
    WITH params AS (
        SELECT *,
            '{{ delimiter_literal }}' AS delimiter,
            {{ noOfColumns }} AS num_cols
        FROM {{ relation_list | join(', ') }}
    ),
    split_result AS (
        SELECT *,
            SPLIT(
                {{ quoted_column_name }},
                '{{ delimiter_literal }}',
                num_cols
            ) AS parts
        FROM params
    ),
    final AS (
        SELECT * EXCEPT(delimiter, num_cols, parts),
        {# Extract tokens positionally (Spark arrays are 0-indexed) #}
        {%- for i in range(1, noOfColumns) %}
            parts[{{ i - 1 }}] AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ i ~ '_' ~ splitColumnSuffix ~ quote_char }},
        {%- endfor %}
            parts[{{ noOfColumns - 1 }}] AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix ~ quote_char }}
        FROM split_result
    )
    SELECT * FROM final

{%- elif split_strategy == 'splitRows' -%}
    {%- set split_rows_except_columns = ['delimiter', 'num_cols', 'parts'] -%}
    {%- if columnNames == splitRowsColumnName -%}
        {%- do split_rows_except_columns.append(quoted_column_name) -%}
    {%- endif -%}
    WITH params AS (
        SELECT *,
            '{{ delimiter_literal }}' AS delimiter,
            {{ noOfColumns }} AS num_cols
        FROM {{ relation_list | join(', ') }}
    ),
    split_result AS (
        SELECT *,
            SPLIT(
                if({{ quoted_column_name }} IS NULL, '', {{ quoted_column_name }}),
                '{{ delimiter_literal }}'
            ) AS parts
        FROM params
    )
    SELECT r.* EXCEPT({{ split_rows_except_columns | join(', ') }}),
            s.col AS {{ quote_char ~ splitRowsColumnName ~ quote_char }}
    FROM split_result r
    LATERAL VIEW explode(parts) s AS col

{%- else -%}
SELECT * FROM {{ relation_list | join(', ') }}
{%- endif -%}

{% endmacro %}

{% macro bigquery__TextToColumns(
    relation_name,
    columnNames,
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
{# BigQuery string literals — including raw r'...' — cannot contain a bare newline or
   carriage return, which raises "Unclosed raw string literal". dbt turns a \n / \r
   delimiter into a real control character, so re-encode those as regex escapes: inside a
   raw string \n and \r are matched as newline / carriage return by the regex engine. #}
{%- set pattern = pattern | replace('\n', '\\n') | replace('\r', '\\r') -%}
{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

{# Helper to quote column names inline #}
{%- set quote_char = '`' -%}

{# Quote the column name properly #}
{%- set quoted_column_name = prophecy_basics.quote_identifier(columnNames) -%}
{# Delimiter is treated as a regex: convert matches to a sentinel then split on it. #}
{%- set leave_extra = (leaveExtraCharLastCol == true or leaveExtraCharLastCol == 'Leave extra in last column' or leaveExtraCharLastCol == 'true' or leaveExtraCharLastCol == 'True') -%}

{%- if split_strategy == 'splitColumns' -%}
    WITH source AS (
        SELECT *,
            SPLIT(
                REGEXP_REPLACE({{ quoted_column_name }}, {{ "r'" ~ pattern ~ "'" }}, '%%DELIM%%'),
                '%%DELIM%%'
            ) AS tokens
        FROM {{ relation_list | join(', ') }}
    ),
    all_data AS (
    SELECT *,
        {# Extract tokens positionally (BigQuery arrays are 0-indexed) #}
        {%- for i in range(1, noOfColumns) %}
            CASE
                WHEN ARRAY_LENGTH(tokens) > {{ i - 1 }}
                    THEN tokens[OFFSET({{ i - 1 }})]
                ELSE null
            END AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ i ~ '_' ~ splitColumnSuffix ~ quote_char }}, {% endfor %}
        {%- if leave_extra %}
            CASE
                WHEN ARRAY_LENGTH(tokens) >= {{ noOfColumns }}
                    THEN ARRAY_TO_STRING(ARRAY(SELECT tokens[OFFSET(i)] FROM UNNEST(GENERATE_ARRAY({{ noOfColumns - 1 }}, ARRAY_LENGTH(tokens) - 1)) AS i), REGEXP_EXTRACT({{ quoted_column_name }}, {{ "r'" ~ pattern ~ "'" }}))
                ELSE null
            END AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix ~ quote_char }}
        {%- else %}
            CASE
                WHEN ARRAY_LENGTH(tokens) > {{ noOfColumns - 1 }}
                    THEN tokens[OFFSET({{ noOfColumns - 1 }})]
                ELSE null
            END AS {{ quote_char ~ splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix ~ quote_char }}
        {%- endif %}
    FROM source
    )
    SELECT * EXCEPT(tokens) FROM all_data

{%- elif split_strategy == 'splitRows' -%}
    SELECT r.*,
        split_value AS {{ quote_char ~ splitRowsColumnName ~ quote_char }}
    FROM {{ relation_list | join(', ') }} r,
    UNNEST(SPLIT(REGEXP_REPLACE(COALESCE(r.{{ quoted_column_name }}, ''), {{ "r'" ~ pattern ~ "'" }}, '%%DELIM%%'), '%%DELIM%%')) AS split_value

{%- else -%}
SELECT * FROM {{ relation_list | join(', ') }}
{%- endif -%}

{% endmacro %}


{% macro snowflake__TextToColumns(
    relation_name,
    columnNames,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName
    ) %}

{%- set pattern = delimiter -%}
{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set quoted_column_name = prophecy_basics.quote_identifier(columnNames) -%}
{%- set delimiter_literal = prophecy_basics.escape_sql_string(pattern, escape_backslashes=true) -%}
{%- set leave_extra = (leaveExtraCharLastCol == true or leaveExtraCharLastCol == 'Leave extra in last column' or leaveExtraCharLastCol == 'true' or leaveExtraCharLastCol == 'True') -%}

{%- if split_strategy == 'splitColumns' -%}
    WITH source AS (
        SELECT *,
            SPLIT(
                REGEXP_REPLACE({{ quoted_column_name }}, '{{ delimiter_literal }}', '%%DELIM%%'),
                '%%DELIM%%'
            ) AS tokens
        FROM {{ relation_list | join(', ') }}
    ),
    all_data AS (
    SELECT *,
        {# SPLIT() returns an ARRAY of VARIANT; cast to STRING so values are plain text
           (no VARIANT quoting artifact) without altering the underlying content. #}
        {%- for i in range(1, noOfColumns) %}
            s.tokens[{{ i - 1 }}]::STRING AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ i ~ '_' ~ splitColumnSuffix) }},
        {%- endfor %}
        {%- if leave_extra %}
            CASE
                WHEN ARRAY_SIZE(s.tokens) >= {{ noOfColumns }}
                    THEN ARRAY_TO_STRING(ARRAY_SLICE(s.tokens, {{ noOfColumns - 1 }}, ARRAY_SIZE(s.tokens)), REGEXP_SUBSTR({{ quoted_column_name }}, '{{ delimiter_literal }}'))
                ELSE NULL
            END AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix) }}
        {%- else %}
            s.tokens[{{ noOfColumns - 1 }}]::STRING AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix) }}
        {%- endif %}
    FROM source AS s
    )
    SELECT * EXCLUDE(tokens) FROM all_data

{%- elif split_strategy == 'splitRows' -%}
    SELECT r.*,
        s.value AS {{ prophecy_basics.quote_identifier(splitRowsColumnName) }}
    FROM {{ relation_list | join(', ') }} r,
    LATERAL SPLIT_TO_TABLE(
        REGEXP_REPLACE(IFF({{ quoted_column_name }} IS NULL, '', {{ quoted_column_name }}), '{{ delimiter_literal }}', '%%DELIM%%'),
        '%%DELIM%%'
    ) s

{%- else -%}
    SELECT * FROM {{ relation_list | join(', ') }}
{%- endif -%}

{% endmacro %}


{%- macro duckdb__TextToColumns(
    relation_name,
    columnNames,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName
) -%}

{%- set pattern = delimiter -%}
{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{%- set quoted_column = prophecy_basics.quote_identifier(columnNames) -%}
{# Delimiter is treated as a regex: convert matches to a sentinel (global) then split on it. #}
{%- set leave_extra = (leaveExtraCharLastCol == true or leaveExtraCharLastCol == 'Leave extra in last column' or leaveExtraCharLastCol == 'true' or leaveExtraCharLastCol == 'True') -%}

{%- if split_strategy == 'splitColumns' -%}
    WITH source AS (
        SELECT *,
            string_split(regexp_replace({{ quoted_column }}, '{{ pattern }}', '%%DELIM%%', 'g'), '%%DELIM%%') AS tokens
        FROM {{ relation_list | join(', ') }}
    ),
    all_data AS (
    SELECT *,
        {# Extract tokens positionally (DuckDB arrays are 1-indexed) #}
        {%- for i in range(1, noOfColumns) %}
            tokens[{{ i }}] AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ i ~ '_' ~ splitColumnSuffix) }},
        {%- endfor %}
        {%- if leave_extra %}
            {%- if noOfColumns == 1 %}
            {# Single column: the whole (non-null) value is the last column. #}
            {{ quoted_column }} AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix) }}
            {%- else %}
            {# Leave-extra: strip the first (noOfColumns-1) "<field><delimiter>" chunks so the
               remainder keeps its ORIGINAL delimiter text. Uses a constant regex — DuckDBs
               array_to_string separator must be constant, so regexp_extract() (per-row) is not
               allowed here. (?s) lets the field span newlines. #}
            CASE
                WHEN array_length(tokens) >= {{ noOfColumns }}
                    THEN regexp_replace({{ quoted_column }}, '(?s)^(.*?{{ pattern }}){{ '{' ~ (noOfColumns - 1) ~ '}' }}', '')
                ELSE null
            END AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix) }}
            {%- endif %}
        {%- else %}
            tokens[{{ noOfColumns }}] AS {{ prophecy_basics.quote_identifier(splitColumnPrefix ~ '_' ~ noOfColumns ~ '_' ~ splitColumnSuffix) }}
        {%- endif %}
    FROM source
    )
    SELECT * EXCLUDE(tokens) FROM all_data

{%- elif split_strategy == 'splitRows' -%}
  SELECT
    r.*,
    s.col AS {{ prophecy_basics.quote_identifier(splitRowsColumnName) }}
  FROM {{ relation_list | join(', ') }} r
  CROSS JOIN UNNEST(
    string_split(regexp_replace(coalesce(r.{{ quoted_column }}, ''), '{{ pattern }}', '%%DELIM%%', 'g'), '%%DELIM%%')
  ) AS s(col)

{%- else -%}
  SELECT * FROM {{ relation_list | join(', ') }}
{%- endif -%}

{%- endmacro -%}
