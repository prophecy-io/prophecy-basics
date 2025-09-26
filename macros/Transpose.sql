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


{%- macro databricks__Transpose(
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


{%- macro snowflake__Transpose(
    relation_name,
    keyColumns,
    dataColumns,
    nameColumn,
    valueColumn,
    schema=[]) -%}

  {%- set available_cols = [] -%}
  {%- if keyColumns and dataColumns and nameColumn | length > 0 and valueColumn | length > 0 -%}
    {%- for col in dataColumns -%}
      {%- if col in schema -%}
        {%- do available_cols.append(col) -%}
      {%- endif -%}
    {%- endfor -%}

    {%- set union_queries = [] -%}
    {%- for data_col in available_cols -%}
      {%- set select_list = [] -%}
      {%- for key in keyColumns -%}
        {%- do select_list.append(key) -%}
      {%- endfor -%}
      {%- do select_list.append("'" ~ data_col ~ "' as " ~ nameColumn) -%}
      {%- do select_list.append('CAST("' ~ data_col ~ '" as string) as ' ~ valueColumn ) -%}
      
      {%- set query = 'SELECT ' ~ (select_list | join(', ')) ~ ' FROM ' ~ relation_name -%}
      {%- do union_queries.append(query) -%}
    {%- endfor -%}

    {{ union_queries | join('\nUNION ALL\n') }}
  {%- else -%}
    select * from {{ relation_name }}
  {%- endif -%}

{%- endmacro -%}