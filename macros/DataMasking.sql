{% macro DataMasking(relation_name,
    column_names,
    remaining_columns,
    masking_method,
    upper_char_substitute,
    lower_char_substitute,
    digit_char_substitute,
    other_char_substitute,
    sha2_bit_length,
    masked_column_add_method,
    prefix_suffix_opt,
    prefix_suffix_val,
    combined_hash_column_name) -%}
    {{ return(adapter.dispatch('DataMasking', 'prophecy_basics')(relation_name,
    column_names,
    remaining_columns,
    masking_method,
    upper_char_substitute,
    lower_char_substitute,
    digit_char_substitute,
    other_char_substitute,
    sha2_bit_length,
    masked_column_add_method,
    prefix_suffix_opt,
    prefix_suffix_val,
    combined_hash_column_name)) }}
{% endmacro %}


{%- macro databricks__DataMasking(
    relation_name,
    column_names,
    remaining_columns,
    masking_method,
    upper_char_substitute,
    lower_char_substitute,
    digit_char_substitute,
    other_char_substitute,
    sha2_bit_length,
    masked_column_add_method,
    prefix_suffix_opt,
    prefix_suffix_val,
    combined_hash_column_name
) %}

    {{ log("Applying Masking-specific column operations", info=True) }}
    {%- set withColumn_clause = [] -%}
    {%- if masking_method == "mask" -%}
        {% for column in column_names %}
            {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
            {%- set args = [quoted_column] -%}

            {%- if upper_char_substitute == "NULL" -%}
                {%- do args.append("upperChar => NULL") -%}
            {%- elif upper_char_substitute != "" -%}
                {%- do args.append("upperChar => '" ~ upper_char_substitute ~ "'") -%}
            {%- endif -%}

            {%- if lower_char_substitute == "NULL" -%}
                {%- do args.append("lowerChar => NULL") -%}
            {%- elif lower_char_substitute != "" -%}
                {%- do args.append("lowerChar => '" ~ lower_char_substitute ~ "'") -%}
            {%- endif -%}

            {%- if digit_char_substitute == "NULL" -%}
                {%- do args.append("digitChar => NULL") -%}
            {%- elif digit_char_substitute != "" -%}
                {%- do args.append("digitChar => '" ~ digit_char_substitute ~ "'") -%}
            {%- endif -%}

            {%- if other_char_substitute == "NULL" -%}
                {%- do args.append("otherChar => NULL") -%}
            {%- elif other_char_substitute != "" -%}
                {%- do args.append("otherChar => '" ~ other_char_substitute ~ "'") -%}
            {%- endif -%}
            {%- set arg_string = args | join(', ') -%}
            {%- if masked_column_add_method == "inplace_substitute" -%}
                {%- do withColumn_clause.append("mask(" ~ arg_string ~ ") AS " ~ prophecy_basics.quote_identifier(column)) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("mask(" ~ arg_string ~ ") AS " ~ prophecy_basics.quote_identifier(prefix_suffix_val ~ column)) -%}
            {%- else -%}
                {%- do withColumn_clause.append("mask(" ~ arg_string ~ ") AS " ~ prophecy_basics.quote_identifier(column ~ prefix_suffix_val)) -%}
            {%- endif -%}
        {% endfor %}

    {%- elif masking_method == "hash" -%}
        {%- if masked_column_add_method == "combinedHash_substitute" -%}
            {%- set quoted_columns = [] -%}
            {%- for column in column_names -%}
                {%- do quoted_columns.append(prophecy_basics.quote_identifier(column)) -%}
            {%- endfor -%}
            {%- set arg_string = quoted_columns | join(', ') -%}
            {%- do withColumn_clause.append("hash(" ~ arg_string ~ ") AS " ~ prophecy_basics.quote_identifier(combined_hash_column_name)) -%}
        {%- else  -%}
            {% for column in column_names %}
                {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
                {%- if masked_column_add_method == "inplace_substitute" -%}
                    {%- do withColumn_clause.append("hash(" ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(column)) -%}
                {%- elif prefix_suffix_opt == "Prefix" -%}
                    {%- do withColumn_clause.append("hash(" ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(prefix_suffix_val ~ column)) -%}
                {%- else -%}
                    {%- do withColumn_clause.append("hash(" ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(column ~ prefix_suffix_val)) -%}
                {%- endif -%}
            {% endfor %}
        {%- endif -%}

    {%- elif masking_method == "sha2" -%}
        {% for column in column_names %}
            {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
            {%- if masked_column_add_method == "inplace_substitute" -%}
                {%- do withColumn_clause.append("sha2(" ~ quoted_column ~ ", " ~ sha2_bit_length ~ ") AS " ~ prophecy_basics.quote_identifier(column)) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("sha2(" ~ quoted_column ~ ", " ~ sha2_bit_length ~ ") AS " ~ prophecy_basics.quote_identifier(prefix_suffix_val ~ column)) -%}
            {%- else -%}
                {%- do withColumn_clause.append("sha2(" ~ quoted_column ~ ", " ~ sha2_bit_length ~ ") AS " ~ prophecy_basics.quote_identifier(column ~ prefix_suffix_val)) -%}
            {%- endif -%}
        {% endfor %}

    {%- else -%}
        {% for column in column_names %}
            {%- set quoted_column = prophecy_basics.quote_identifier(column) -%}
            {%- if masked_column_add_method == "inplace_substitute" -%}
                {%- do withColumn_clause.append(masking_method ~ "(" ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(column)) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append(masking_method ~ "(" ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(prefix_suffix_val ~ column)) -%}
            {%- else -%}
                {%- do withColumn_clause.append(masking_method ~ "(" ~ quoted_column ~ ") AS " ~ prophecy_basics.quote_identifier(column ~ prefix_suffix_val)) -%}
            {%- endif -%}
        {% endfor %}

    {%- endif -%}

    {%- set select_clause_sql = withColumn_clause | join(', ') -%}

    {%- set select_cte_sql -%}
        {%- if select_clause_sql == "" -%}
            WITH final_cte AS (
                SELECT *
                FROM {{ relation_name }}
            )
        {%- elif (masked_column_add_method == "prefix_suffix_substitute") or (masking_method == "hash" and masked_column_add_method == "combinedHash_substitute") -%}
            WITH final_cte AS (
                SELECT *, {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- elif remaining_columns == "" -%}
            WITH final_cte AS (
                SELECT {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- else -%}
            WITH final_cte AS (
                SELECT {{ remaining_columns }}, {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_cte_sql ~ "\nSELECT * FROM final_cte" -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro -%}
