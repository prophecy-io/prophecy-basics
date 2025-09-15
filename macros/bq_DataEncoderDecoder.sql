-- AES Encryption/Decryption Removed from BigQuery Implementation
-- Implementation Requires Secret Manager
{% macro DataEncoderDecoder(
    relation_name,
    column_names,
    remaining_columns,
    enc_dec_method,
    enc_dec_charSet,
    prefix_suffix_opt,
    change_col_name,
    prefix_suffix_val
) %}
    {{ log("Applying encoding-specific column operations", info=True) }}
    {%- set withColumn_clause = [] -%}
    {%- if enc_dec_method == "base64" -%}
        {% for column in column_names %}
            {%- if change_col_name == "inplace_substitute" -%}
                {%- do withColumn_clause.append("TO_BASE64(CAST(" ~ column ~ " AS BYTES)) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("TO_BASE64(CAST(" ~ column ~ " AS BYTES)) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("TO_BASE64(CAST(" ~ column ~ " AS BYTES)) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "unbase64" -%}
        {% for column in column_names %}
            {%- if change_col_name == "inplace_substitute" -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_BASE64(" ~ column ~ ") AS STRING) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_BASE64(" ~ column ~ ") AS STRING) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_BASE64(" ~ column ~ ") AS STRING) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "hex" -%}
        {% for column in column_names %}
            {%- if change_col_name == "inplace_substitute" -%}
                {%- do withColumn_clause.append("TO_HEX(CAST(" ~ column ~ " AS BYTES)) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("TO_HEX(CAST(" ~ column ~ " AS BYTES)) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("TO_HEX(CAST(" ~ column ~ " AS BYTES)) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "unhex" -%}
        {% for column in column_names %}
            {%- if change_col_name == "inplace_substitute" -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_HEX(" ~ column ~ ") AS STRING) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_HEX(" ~ column ~ ") AS STRING) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_HEX(" ~ column ~ ") AS STRING) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "encode" -%}
        {% for column in column_names %}
            {%- if change_col_name == "inplace_substitute" -%}
                {%- do withColumn_clause.append("TO_HEX(CAST(" ~ column ~ " AS BYTES)) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("TO_HEX(CAST(" ~ column ~ " AS BYTES)) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("TO_HEX(CAST(" ~ column ~ " AS BYTES)) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "decode" -%}
        {% for column in column_names %}
            {%- if change_col_name == "inplace_substitute" -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_HEX(" ~ column ~ ") AS STRING) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_HEX(" ~ column ~ ") AS STRING) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("CAST(" ~ "FROM_HEX(" ~ column ~ ") AS STRING) AS " ~ column ~ prefix_suffix_val) -%}
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
        {%- elif change_col_name == "prefix_suffix_substitute" -%}
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
{%- endmacro %}