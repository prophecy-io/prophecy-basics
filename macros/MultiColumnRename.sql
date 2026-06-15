{#
  MultiColumnRename Macro Gem
  ===========================

  Renames multiple columns in one step—by adding a prefix or suffix, or by a
  Python expression per column—while keeping the rest of the schema in the same
  order as your metadata list.

  Parameters:
    - relation_name (list): Source relation(s).
    - columnNames (list): Columns to rename.
    - renameMethod: 'editPrefixSuffix' (needs editType 'Prefix' or suffix) or 'advancedRename'
        (evaluate customExpression with column_name replaced per column).
    - schema (list): All output column names in order (strings or objects per adapter).
    - editType, editWith: For editPrefixSuffix — prefix or suffix string.
    - customExpression: Python expression evaluated to new name for advancedRename.

  Adapter Support:
    - default__, snowflake__, duckdb__ (duckdb advancedRename uses quoted string alias)

  Depends on schema parameter:
    Yes

  Macro Call Examples (default__):
    {{ prophecy_basics.MultiColumnRename(['t'], ['old'], 'editPrefixSuffix', schema_list, 'Prefix', 'new_') }}
    {{ prophecy_basics.MultiColumnRename(['t'], ['col'], 'advancedRename', schema_list, '', '', "'ren_' + column_name") }}

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.MultiColumnRename(['t'], ['old'], 'editPrefixSuffix', schema_list, 'Prefix', 'new_') }}

    Resolved query (default__ — illustrative; full SELECT list follows schema order):
      select
          `old` AS `new_old`,
          `other_col`
      from t
#}
{% macro MultiColumnRename(relation_name,
    columnNames,
    renameMethod,
    schema,
    editType = '',
    editWith = '',
    customExpression='') -%}
    {{ return(adapter.dispatch('MultiColumnRename', 'prophecy_basics')(relation_name,
    columnNames,
    renameMethod,
    schema,
    editType,
    editWith,
    customExpression)) }}
{% endmacro %}


{% macro default__MultiColumnRename(
    relation_name,
    columnNames,
    renameMethod,
    schema,
    editType = '',
    editWith = '',
    customExpression='')
%}
    {%- set renamed_columns = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {%- for column in columnNames -%}
        {%- set renamed_column = "" -%}
        {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
        
        {%- if renameMethod == 'editPrefixSuffix' -%}
                {%- if editType == 'Prefix' -%}
                    {%- set renamed_column = quoted_column ~ " AS " ~ prophecy_basics.quote_identifier(editWith ~ column) -%}
                {%- else -%}
                    {%- set renamed_column = quoted_column ~ " AS " ~ prophecy_basics.quote_identifier(column ~ editWith) -%}
                {%- endif -%}
        {%- elif renameMethod == 'advancedRename' -%}
                {%- set custom_expr_result = prophecy_basics.evaluate_expression(customExpression | replace('column_name',  "\'" ~ column ~ "\'"),column) -%}
                {%- set custom_expr_result_trimmed = custom_expr_result | trim -%}
                {%- set renamed_column = quoted_column ~ " AS " ~ prophecy_basics.quote_identifier(custom_expr_result_trimmed) -%}
        {%- endif -%}
        
        {%- do renamed_columns.append(renamed_column) -%}
    {%- endfor -%}

    {# Get the schema of cleansed data #}
    {%- set output_columns = [] -%}
    {%- for col_name_val in schema -%}
        {% set flag_dict = {"flag": false} %}
        {%- for expr in renamed_columns -%}
            {# Split on 'AS' to get the orig column name; assumes expression contains "AS" #}
            {%- set parts = expr.split(' AS ') -%}
            {%- set orig_col_name = parts[0] | trim -%}
            {%- set unquoted_orig_col = prophecy_basics.unquote_identifier(orig_col_name) | upper -%}
            {%- set unquoted_schema_col = prophecy_basics.unquote_identifier(col_name_val) | upper -%}
            
            {%- if unquoted_schema_col == unquoted_orig_col -%}
                {%- do output_columns.append(expr) -%}
                {% do flag_dict.update({"flag": true}) %}
                {%- break -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if flag_dict.flag == false -%}    
            {%- do output_columns.append(prophecy_basics.quote_identifier(col_name_val)) -%}
        {%- endif -%}
    {%- endfor -%}

    select 
        {{ output_columns | join(',\n    ') }}
    from {{ relation_list | join(', ') }}
{% endmacro %}



{% macro snowflake__MultiColumnRename(
    relation_name,
    columnNames,
    renameMethod,
    schema,
    editType = '',
    editWith = '',
    customExpression='')
%}
    {%- set renamed_columns = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {%- for column in columnNames -%}
        {%- set renamed_column = "" -%}
        {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}

        {%- if renameMethod == 'editPrefixSuffix' -%}
                {%- if editType == 'Prefix' -%}
                    {%- set renamed_column = quoted_column ~ " AS " ~ prophecy_basics.quote_identifier(editWith ~ column) -%}
                {%- else -%}
                    {%- set renamed_column = quoted_column ~ " AS " ~ prophecy_basics.quote_identifier(column ~ editWith) -%}
                {%- endif -%}
        {%- elif renameMethod == 'advancedRename' -%}
                {%- set custom_expr_result = prophecy_basics.evaluate_expression(customExpression | replace('column_name', "'" ~ column ~ "'"), column) -%}
                {%- set custom_expr_result_trimmed = custom_expr_result | trim -%}
                {%- set renamed_column = quoted_column ~ " AS " ~ prophecy_basics.quote_identifier(custom_expr_result_trimmed) -%}
        {%- endif -%}

        {%- do renamed_columns.append(renamed_column) -%}
    {%- endfor -%}

    {%- set output_columns = [] -%}
    {%- for col_name_val in schema -%}
        {% set flag_dict = {"flag": false} %}
        {%- for expr in renamed_columns -%}
            {%- set parts = expr.split(' AS ') -%}
            {%- set orig_col_name = parts[0] | trim -%}
            {%- set unquoted_orig_col = prophecy_basics.unquote_identifier(orig_col_name) | upper -%}
            {%- set unquoted_schema_col = prophecy_basics.unquote_identifier(col_name_val) | upper -%}

            {%- if unquoted_schema_col == unquoted_orig_col -%}
                {%- do output_columns.append(expr) -%}
                {% do flag_dict.update({"flag": true}) %}
                {%- break -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if flag_dict.flag == false -%}
            {%- do output_columns.append(prophecy_basics.quote_identifier(col_name_val)) -%}
        {%- endif -%}
    {%- endfor -%}

    select
        {{ output_columns | join(',\n    ') }}
    from {{ relation_list | join(', ') }}
{% endmacro %}

{% macro duckdb__MultiColumnRename(
    relation_name,
    columnNames,
    renameMethod,
    schema,
    editType = '',
    editWith = '',
    customExpression='')
%}
    {%- set renamed_columns = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {%- for column in columnNames -%}
        {%- set renamed_column = "" -%}
        {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
        
        {%- if renameMethod == 'editPrefixSuffix' -%}
                {%- if editType == 'Prefix' -%}
                    {%- set renamed_column = quoted_column ~ " AS " ~ prophecy_basics.quote_identifier(editWith ~ column) -%}
                {%- else -%}
                    {%- set renamed_column = quoted_column ~ " AS " ~ prophecy_basics.quote_identifier(column ~ editWith) -%}
                {%- endif -%}
        {%- elif renameMethod == 'advancedRename' -%}
                {%- set custom_expr_result = prophecy_basics.evaluate_expression(customExpression | replace('column_name', "'" ~ column ~ "'"), column) -%}
                {%- set custom_expr_result_trimmed = custom_expr_result | trim -%}
                {%- set quoted_result = "'" ~ custom_expr_result_trimmed ~ "'" -%}
                {%- set renamed_column = column ~ " AS " ~ quoted_result -%}
        {%- endif -%}
        
        {%- do renamed_columns.append(renamed_column) -%}
    {%- endfor -%}

    {# Get the schema of cleansed data #}
    {%- set output_columns = [] -%}
    {%- for col_name_val in schema -%}
        {% set flag_dict = {"flag": false} %}
        {%- for expr in renamed_columns -%}
            {# Split on 'AS' to get the orig column name; assumes expression contains "AS" #}
            {%- set parts = expr.split(' AS ') -%}
            {%- set orig_col_name = parts[0] | trim -%}
            {%- set unquoted_orig_col = prophecy_basics.unquote_identifier(orig_col_name) | upper -%}
            {%- set unquoted_schema_col = prophecy_basics.unquote_identifier(col_name_val) | upper -%}
            
            {%- if unquoted_schema_col == unquoted_orig_col -%}
                {%- do output_columns.append(expr) -%}
                {% do flag_dict.update({"flag": true}) %}
                {%- break -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if flag_dict.flag == false -%}    
            {%- do output_columns.append(prophecy_basics.quote_identifier(col_name_val)) -%}
        {%- endif -%}
    {%- endfor -%}

    select 
        {{ output_columns | join(',\n    ') }}
    from {{ relation_list | join(', ') }}
{% endmacro %}