-- Test: DataMasking macro - output column layout (Databricks)
-- Validates which columns come back, and in what order, for each way of adding
-- masked columns: replace in place, add with a prefix or suffix, combined hash.
-- Runs on Databricks.
--
-- Prefix/suffix and combined hash keep every original column unchanged and
-- append the new columns. In place masks every column here, because the gem
-- only fills remaining_columns from the upstream schema in the SQL Editor.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY VIEW data_masking_columns_src AS
SELECT * FROM VALUES (1, 'John Doe 42', 'jd@x.io') AS t(id, full_name, email)
{% endset %}
{% do run_query(create_src) %}

{% set cases = [] %}

-- in place replaces each column under its own name
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_columns_src'],
    column_names=['full_name', 'email'],
    remaining_columns='',
    masking_method='md5',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='inplace_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='',
    combined_hash_column_name=''
) }}) AS masked
{% endset %}
{% do cases.append({"name": "in place replaces each column under its own name", "columns": ["full_name", "email"], "expect": {"full_name": ["3bd2e7d3239609f8266391d5c729207f"], "email": ["e194413f0765a1c32d46cbbb10e88f87"]}, "sql": macro_query}) %}

-- prefix keeps the originals and appends prefixed columns
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_columns_src'],
    column_names=['full_name'],
    remaining_columns='',
    masking_method='md5',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Prefix',
    prefix_suffix_val='masked_',
    combined_hash_column_name=''
) }}) AS masked ORDER BY id
{% endset %}
{% do cases.append({"name": "prefix keeps the originals and appends prefixed columns", "columns": ["id", "full_name", "email", "masked_full_name"], "expect": {"full_name": ["John Doe 42"], "masked_full_name": ["3bd2e7d3239609f8266391d5c729207f"]}, "sql": macro_query}) %}

-- suffix keeps the originals and appends suffixed columns
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_columns_src'],
    column_names=['full_name'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_masked',
    combined_hash_column_name=''
) }}) AS masked ORDER BY id
{% endset %}
{% do cases.append({"name": "suffix keeps the originals and appends suffixed columns", "columns": ["id", "full_name", "email", "full_name_masked"], "expect": {"full_name": ["John Doe 42"], "full_name_masked": ["Xxxx Xxx nn"]}, "sql": macro_query}) %}

-- combined hash keeps every column and appends one
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_columns_src'],
    column_names=['full_name', 'email'],
    remaining_columns='',
    masking_method='hash',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='combinedHash_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='',
    combined_hash_column_name='row_hash'
) }}) AS masked ORDER BY id
{% endset %}
{% do cases.append({"name": "combined hash keeps every column and appends one", "columns": ["id", "full_name", "email", "row_hash"], "expect": {"full_name": ["John Doe 42"]}, "sql": macro_query}) %}

{% for case in cases %}
    {% set results = run_query(case.sql) %}
    {% if case.columns is defined and (results.column_names | list) != case.columns %}
        {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | list %}
        {% if expected == "ref" %}
            {# integer hash: compare with the database's own function called on the source #}
            {% set expected = run_query("SELECT " ~ case.ref ~ " FROM data_masking_columns_src ORDER BY id").columns[0].values() | list %}
            {% set ok = actual == expected and actual[0] is not none %}
        {% else %}
            {% set ok = actual == expected %}
        {% endif %}
        {% if not ok %}
            {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED on column " ~ col ~ ": expected " ~ expected ~ ", got " ~ actual) }}
        {% endif %}
    {% endfor %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
