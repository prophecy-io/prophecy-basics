{#
  Transpose Macro Gem
  ===================

  Unpivots listed data columns into name/value pairs via UNION ALL of SELECTs.

  Parameters:
    - relation_name (string or list): Source relation(s).
    - keyColumns (list): Identifier columns repeated in each union branch.
    - dataColumns (list): Columns to unpivot; each becomes a row with literal name + CAST value AS STRING.
    - nameColumn, valueColumn: Output column names for the unpivoted pair.
    - schema, customNames: Reserved for future use in default__ (customNames unused in default__).

  Adapter Support:
    - default__ (backticks, CAST AS STRING), snowflake__, duckdb__ (CAST AS VARCHAR)

  Macro Call Examples (default__):
    {{ prophecy_basics.Transpose('t', ['id'], ['q1','q2'], 'metric', 'amount', [], false) }}
#}
{% macro Transpose(relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema=[],
        customNames=false) -%}
    {{ return(adapter.dispatch('Transpose', 'prophecy_basics')(relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema,
        customNames)) }}
{% endmacro %}


{%- macro default__Transpose(
        relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema=[],
        customNames=false
) -%}

    {% set bt = "`" %}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if dataColumns and (nameColumn | length > 0) and (valueColumn | length > 0) -%}

        {%- set union_queries = [] -%}

        {%- for data_col in dataColumns -%}
            {%- set select_list = [] -%}

            {# key columns (if any) #}
            {%- if keyColumns -%}
                {%- for key in keyColumns -%}
                    {%- do select_list.append(bt ~ key ~ bt) %}
                {%- endfor -%}
            {%- endif -%}

            {# literal column name → nameColumn alias #}
            {%- do select_list.append("'" ~ data_col ~ "' AS " ~ bt ~ nameColumn ~ bt) -%}

            {# actual value → valueColumn alias #}
            {%- do select_list.append(
                    'CAST(' ~ bt ~ data_col ~ bt ~ ' AS STRING) AS ' ~ bt ~ valueColumn ~ bt
                ) -%}

            {%- set query = 'SELECT ' ~ (select_list | join(', ')) ~
                            ' FROM ' ~ (relation_list | join(', ')) -%}
            {%- do union_queries.append(query) -%}
        {%- endfor -%}

        {{ union_queries | join('\nUNION ALL\n') }}

    {%- else -%}
        SELECT * FROM {{ relation_list | join(', ') }}
    {%- endif -%}

{%- endmacro -%}

{%- macro snowflake__Transpose(
        relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema=[],
        customNames=false
) -%}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {%- if dataColumns and (nameColumn | length > 0) and (valueColumn | length > 0) -%}

        {%- set union_queries = [] -%}

        {%- for data_col in dataColumns -%}
            {%- set select_list = [] -%}

            {# key columns (if any) #}
            {%- if keyColumns -%}
                {%- for key in keyColumns -%}
                    {%- do select_list.append(prophecy_basics.quote_identifier(key)) %}
                {%- endfor -%}
            {%- endif -%}

            {# literal column name → nameColumn alias #}
            {%- do select_list.append("'" ~ data_col ~ "' AS " ~ prophecy_basics.quote_identifier(nameColumn)) -%}

            {# actual value → valueColumn alias #}
            {%- do select_list.append(
                    'CAST(' ~ prophecy_basics.quote_identifier(data_col) ~ ' AS VARCHAR) AS ' ~ prophecy_basics.quote_identifier(valueColumn)
                ) -%}

            {%- set query = 'SELECT ' ~ (select_list | join(', ')) ~
                            ' FROM ' ~ (relation_list | join(', ')) -%}
            {%- do union_queries.append(query) -%}
        {%- endfor -%}

        {{ union_queries | join('\nUNION ALL\n') }}

    {%- else -%}
        SELECT * FROM {{ relation_list | join(', ') }}
    {%- endif -%}

{%- endmacro -%}

{%- macro duckdb__Transpose(
        relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema=[],
        customNames=false
) -%}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {%- if dataColumns and (nameColumn | length > 0) and (valueColumn | length > 0) -%}

        {%- set union_queries = [] -%}

        {%- for data_col in dataColumns -%}
            {%- set select_list = [] -%}

            {# key columns (if any) #}
            {%- if keyColumns -%}
                {%- for key in keyColumns -%}
                    {%- do select_list.append(prophecy_basics.quote_identifier(key)) %}
                {%- endfor -%}
            {%- endif -%}

            {# literal column name → nameColumn alias #}
            {%- do select_list.append("'" ~ data_col ~ "' AS " ~ prophecy_basics.quote_identifier(nameColumn)) -%}

            {# actual value → valueColumn alias #}
            {%- do select_list.append(
                    'CAST(' ~ prophecy_basics.quote_identifier(data_col) ~ ' AS VARCHAR) AS ' ~ prophecy_basics.quote_identifier(valueColumn)
                ) -%}

            {%- set query = 'SELECT ' ~ (select_list | join(', ')) ~
                            ' FROM ' ~ (relation_list | join(', ')) -%}
            {%- do union_queries.append(query) -%}
        {%- endfor -%}

        {{ union_queries | join('\nUNION ALL\n') }}

    {%- else -%}
        SELECT * FROM {{ relation_list | join(', ') }}
    {%- endif -%}

{%- endmacro -%}