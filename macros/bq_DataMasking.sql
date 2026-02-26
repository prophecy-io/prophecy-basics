-- CRC32 & Certain SHA2 variants (SHA224, SHA384, SHA512) are not available in BigQuery
{%- macro DataMasking(
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

    {{ log("Applying BigQuery Masking-specific column operations", info=True) }}
    {%- set withColumn_clause = [] -%}
    
    {%- if masking_method == "mask" -%}
        {% for column in column_names %}
            {%- set mask_expression -%}
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                {{ column }},
                                '[A-Z]', 
                                {% if upper_char_substitute == "NULL" %}''{% else %}'{{ upper_char_substitute }}'{% endif %}
                            ),
                            '[a-z]', 
                            {% if lower_char_substitute == "NULL" %}''{% else %}'{{ lower_char_substitute }}'{% endif %}
                        ),
                        '[0-9]', 
                        {% if digit_char_substitute == "NULL" %}''{% else %}'{{ digit_char_substitute }}'{% endif %}
                    ),
                    '[^A-Za-z0-9]', 
                    {% if other_char_substitute == "NULL" or other_char_substitute is none %}''{% else %}'{{ other_char_substitute }}'{% endif %}
                )
            {%- endset -%}
            
            {%- if masked_column_add_method == "inplace_substitute" -%}
                {%- do withColumn_clause.append(mask_expression ~ " AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append(mask_expression ~ " AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append(mask_expression ~ " AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}

    {%- elif masking_method == "hash" -%}
        {%- if masked_column_add_method == "combinedHash_substitute" -%}
            {%- set combined_columns = column_names | join(" || ") -%}
            {%- do withColumn_clause.append("TO_HEX(SHA256(CAST(" ~ combined_columns ~ " AS BYTES))) AS " ~ combined_hash_column_name) -%}
        {%- else  -%}
            {% for column in column_names %}
                {%- if masked_column_add_method == "inplace_substitute" -%}
                    {%- do withColumn_clause.append("TO_HEX(SHA256(CAST(" ~ column ~ " AS BYTES))) AS " ~ column) -%}
                {%- elif prefix_suffix_opt == "Prefix" -%}
                    {%- do withColumn_clause.append("TO_HEX(SHA256(CAST(" ~ column ~ " AS BYTES))) AS " ~ prefix_suffix_val ~ column) -%}
                {%- else -%}
                    {%- do withColumn_clause.append("TO_HEX(SHA256(CAST(" ~ column ~ " AS BYTES))) AS " ~ column ~ prefix_suffix_val) -%}
                {%- endif -%}
            {% endfor %}
        {%- endif -%}

    {%- elif masking_method == "sha" -%}
        {% for column in column_names %}
            {%- if masked_column_add_method == "inplace_substitute" -%}
                {%- do withColumn_clause.append("TO_HEX(SHA1(CAST(" ~ column ~ " AS BYTES))) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("TO_HEX(SHA1(CAST(" ~ column ~ " AS BYTES))) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("TO_HEX(SHA1(CAST(" ~ column ~ " AS BYTES))) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}

    {%- elif masking_method == "sha2" -%}
        {% for column in column_names %}
            {%- set sha2_function -%}
                {%- if sha2_bit_length == "256" -%}
                    TO_HEX(SHA256(CAST({{ column }} AS BYTES)))
                {%- else -%}
                    TO_HEX(SHA256(CAST({{ column }} AS BYTES)))
                {%- endif -%}
            {%- endset -%}
            
            {%- if masked_column_add_method == "inplace_substitute" -%}
                {%- do withColumn_clause.append(sha2_function ~ " AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append(sha2_function ~ " AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append(sha2_function ~ " AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}

    {%- elif masking_method == "md5" -%}
        {% for column in column_names %}
            {%- if masked_column_add_method == "inplace_substitute" -%}
                {%- do withColumn_clause.append("TO_HEX(MD5(CAST(" ~ column ~ " AS BYTES))) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("TO_HEX(MD5(CAST(" ~ column ~ " AS BYTES))) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("TO_HEX(MD5(CAST(" ~ column ~ " AS BYTES))) AS " ~ column ~ prefix_suffix_val) -%}
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
