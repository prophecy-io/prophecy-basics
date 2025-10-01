{% macro Transpose(relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema=[]) -%}
    {{ return(adapter.dispatch('Transpose', 'prophecy_basics')(relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema)) }}
{% endmacro %}


{%- macro default__Transpose(
        relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema=[]
) -%}

    {% set bt = "`" %}

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
                            ' FROM ' ~ relation_name -%}
            {%- do union_queries.append(query) -%}
        {%- endfor -%}

        {{ union_queries | join('\nUNION ALL\n') }}

    {%- else -%}
        SELECT * FROM {{ relation_name }}
    {%- endif -%}

{%- endmacro -%}

{%- macro duckdb__Transpose(
        relation_name,
        keyColumns,
        dataColumns,
        nameColumn,
        valueColumn,
        schema=[]
) -%}

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
                            ' FROM ' ~ relation_name -%}
            {%- do union_queries.append(query) -%}
        {%- endfor -%}

        {{ union_queries | join('\nUNION ALL\n') }}

    {%- else -%}
        SELECT * FROM {{ relation_name }}
    {%- endif -%}

{%- endmacro -%}