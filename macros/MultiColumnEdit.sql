{#
  MultiColumnEdit Macro Gem
  =========================

  Applies the same kind of expression to many columns at once—uppercasing, math,
  concatenation, or any SQL you template—using placeholders for the current column
  name and value. You can replace columns in place or emit new names with a prefix
  or suffix.

  Parameters:
    - relation_name (list): Source relation(s).
    - expressionToBeApplied: SQL fragment with column_value and column_name tokens.
    - allColumnNames (list): Full column list in output order.
    - columnNames (list): Subset to transform when changeOutputFieldName is false.
    - changeOutputFieldName (bool): If true, emit all allColumnNames as-is then append transformed
        columns with prefix/suffix aliases; if false, replace in place for columnNames only.
    - prefixSuffixOption: "prefix" or other (suffix) when changeOutputFieldName is true.
    - prefixSuffixToBeAdded: Prefix or suffix string for new names.

  Adapter Support:
    - default__, snowflake__, duckdb__ (quoting differences for column_name replacement)

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {{ prophecy_basics.MultiColumnEdit('t', 'upper(column_value)', ['a','b'], ['a'], False, 'prefix', '') }}
    {{ prophecy_basics.MultiColumnEdit('t', 'concat(column_name, column_value)', ['a','b'], ['a'], True, 'prefix', 'new_') }}

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.MultiColumnEdit('t', 'upper(column_value)', ['a','b'], ['a'], False, 'prefix', '') }}

    Resolved query (default__):
      select
          upper(`a`) as `a`,
          `b`
      from t
#}
{% macro MultiColumnEdit(relation_name,
    expressionToBeApplied,
    allColumnNames=[],
    columnNames=[],
    changeOutputFieldName=false,
    prefixSuffixOption = 'prefix / suffix',
    prefixSuffixToBeAdded='') -%}
    {{ return(adapter.dispatch('MultiColumnEdit', 'prophecy_basics')(relation_name,
    expressionToBeApplied,
    allColumnNames,
    columnNames,
    changeOutputFieldName,
    prefixSuffixOption,
    prefixSuffixToBeAdded)) }}
{% endmacro %}


{%- macro default__MultiColumnEdit(
    relation_name,
    expressionToBeApplied,
    allColumnNames=[],
    columnNames=[],
    changeOutputFieldName=false,
    prefixSuffixOption = 'prefix / suffix',
    prefixSuffixToBeAdded=''
) -%}

    {%- set select_expressions = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if changeOutputFieldName -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- do select_expressions.append(quoted_col) -%}
        {%- endfor -%}
        {%- for col in columnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- set col_name_replacement = '"' ~ col ~ '"' -%}
            {%- if prefixSuffixOption | lower == "prefix" -%}
                {%- set alias = prefixSuffixToBeAdded ~ col -%}
            {%- else -%}
                {%- set alias = col ~ prefixSuffixToBeAdded -%}
            {%- endif -%}
            {%- set quoted_alias = prophecy_basics.quote_identifier(alias) -%}
            {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', col_name_replacement) -%}
            {%- do select_expressions.append(expr ~ ' as ' ~ quoted_alias) -%}
        {%- endfor -%}
    {%- else -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- set col_name_replacement = '"' ~ col ~ '"' -%}
            {%- if col in columnNames -%}
                {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', col_name_replacement) -%}
                {%- do select_expressions.append(expr ~ ' as ' ~ quoted_col) -%}
            {%- else -%}
                {%- do select_expressions.append(quoted_col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    select {{ select_expressions | join(',\n        ') }} from {{ relation_list | join(', ') }}

{%- endmacro -%}

{%- macro snowflake__MultiColumnEdit(
    relation_name,
    expressionToBeApplied,
    allColumnNames=[],
    columnNames=[],
    changeOutputFieldName=false,
    prefixSuffixOption = 'prefix / suffix',
    prefixSuffixToBeAdded=''
) -%}

    {%- set select_expressions = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if changeOutputFieldName -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- do select_expressions.append(quoted_col) -%}
        {%- endfor -%}
        {%- for col in columnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- set col_name_replacement = prophecy_basics.quote_identifier(col) -%}
            {%- if prefixSuffixOption | lower == "prefix" -%}
                {%- set alias = prophecy_basics.quote_identifier(prefixSuffixToBeAdded ~ col) -%}
            {%- else -%}
                {%- set alias = prophecy_basics.quote_identifier(col ~ prefixSuffixToBeAdded) -%}
            {%- endif -%}
            {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', col_name_replacement) -%}
            {%- do select_expressions.append(expr ~ ' as ' ~ alias) -%}
        {%- endfor -%}
    {%- else -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- set col_name_replacement = prophecy_basics.quote_identifier(col) -%}
            {%- if col in columnNames -%}
                {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', col_name_replacement) -%}
                {%- do select_expressions.append(expr ~ ' as ' ~ quoted_col) -%}
            {%- else -%}
                {%- do select_expressions.append(quoted_col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    select {{ select_expressions | join(',\n        ') }} from {{ relation_list | join(', ') }}

{%- endmacro -%}

{%- macro duckdb__MultiColumnEdit(
    relation_name,
    expressionToBeApplied,
    allColumnNames=[],
    columnNames=[],
    changeOutputFieldName=false,
    prefixSuffixOption = 'prefix / suffix',
    prefixSuffixToBeAdded=''
) -%}

    {%- set select_expressions = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

    {%- if changeOutputFieldName -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- do select_expressions.append(quoted_col) -%}
        {%- endfor -%}
        {%- for col in columnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- set col_name_replacement = '"' ~ col ~ '"' -%}
            {%- if prefixSuffixOption | lower == "prefix" -%}
                {%- set alias = prophecy_basics.quote_identifier(prefixSuffixToBeAdded ~ col) -%}
            {%- else -%}
                {%- set alias = prophecy_basics.quote_identifier(col ~ prefixSuffixToBeAdded) -%}
            {%- endif -%}
            {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', col_name_replacement) -%}
            {%- do select_expressions.append(expr ~ ' as ' ~ alias) -%}
        {%- endfor -%}
    {%- else -%}
        {%- for col in allColumnNames -%}
            {%- set quoted_col = prophecy_basics.quote_identifier(col) -%}
            {%- set col_name_replacement = '"' ~ col ~ '"' -%}
            {%- if col in columnNames -%}
                {%- set expr = expressionToBeApplied | replace('column_value', quoted_col) | replace('column_name', col_name_replacement) -%}
                {%- do select_expressions.append(expr ~ ' as ' ~ quoted_col) -%}
            {%- else -%}
                {%- do select_expressions.append(quoted_col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    select {{ select_expressions | join(',\n        ') }} from {{ relation_list | join(', ') }}

{%- endmacro -%}
