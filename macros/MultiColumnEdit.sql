{% macro MultiColumnEdit(relation,
    expressionToBeApplied,
    allColumnNames=[],
    columnNames=[],
    changeOutputFieldName=false,
    prefixSuffixOption = 'prefix / suffix',
    prefixSuffixToBeAdded='') -%}
    {{ return(adapter.dispatch('MultiColumnEdit', 'prophecy_basics')(relation,
    expressionToBeApplied,
    allColumnNames,
    columnNames,
    changeOutputFieldName,
    prefixSuffixOption,
    prefixSuffixToBeAdded)) }}
{% endmacro %}


{%- macro databricks__MultiColumnEdit(
    relation,
    expressionToBeApplied,
    allColumnNames=[],
    columnNames=[],
    changeOutputFieldName=false,
    prefixSuffixOption = 'prefix / suffix',
    prefixSuffixToBeAdded=''
) -%}

    {%- set select_expressions = [] -%}

    {%- if changeOutputFieldName -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- do select_expressions.append(quoted_col) -%}
        {%- endfor -%}
        {%- for col in columnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- if prefixSuffixOption | lower == "prefix" -%}
                {%- set alias = prefixSuffixToBeAdded ~ col -%}
            {%- else -%}
                {%- set alias = col ~ prefixSuffixToBeAdded -%}
            {%- endif -%}
            {%- set quoted_alias = prophecy_basics.quote_identifier(alias) -%}
            {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', quoted_col) -%}
            {%- do select_expressions.append(expr ~ ' as ' ~ quoted_alias) -%}
        {%- endfor -%}
    {%- else -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- if col in columnNames -%}
                {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', quoted_col) -%}
                {%- do select_expressions.append(expr ~ ' as ' ~ quoted_col) -%}
            {%- else -%}
                {%- do select_expressions.append(quoted_col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    select {{ select_expressions | join(',\n        ') }} from {{ relation }}

{%- endmacro -%}

{%- macro duckdb__MultiColumnEdit(
    relation,
    expressionToBeApplied,
    allColumnNames=[],
    columnNames=[],
    changeOutputFieldName=false,
    prefixSuffixOption = 'prefix / suffix',
    prefixSuffixToBeAdded=''
) -%}

    {%- set select_expressions = [] -%}

    {%- if changeOutputFieldName -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- do select_expressions.append(quoted_col) -%}
        {%- endfor -%}
        {%- for col in columnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- if prefixSuffixOption | lower == "prefix" -%}
                {%- set alias = prophecy_basics.quote_identifier(prefixSuffixToBeAdded ~ col) -%}
            {%- else -%}
                {%- set alias = prophecy_basics.quote_identifier(col ~ prefixSuffixToBeAdded) -%}
            {%- endif -%}
            {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', quoted_col) -%}
            {%- do select_expressions.append(expr ~ ' as ' ~ alias) -%}
        {%- endfor -%}
    {%- else -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- if col in columnNames -%}
                {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', quoted_col) -%}
                {%- do select_expressions.append(expr ~ ' as ' ~ quoted_col) -%}
            {%- else -%}
                {%- do select_expressions.append(quoted_col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    select {{ select_expressions | join(',\n        ') }} from {{ relation }}

{%- endmacro -%}
