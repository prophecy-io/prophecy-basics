-- Test: DataMasking macro - mask method (DuckDB)
-- Validates character masking: default substitutes, "NULL" to keep a category,
-- custom substitutes, other-character masking and NULL passthrough.
-- Runs on DuckDB; the duckdb__ adapter builds a regexp_replace chain.
--
-- Each case runs the macro on a two-row source (row 2 is all NULL) and compares
-- every listed column, ordered by id, against the expected values.
--
-- Every call passes remaining_columns='' so it round-trips through the
-- code-to-visual tester, which has no input schema to rebuild that list from.
-- Value checks therefore add a "_masked" suffix column next to the originals.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE data_masking_mask_src AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io'), (2, NULL, NULL)) AS t(id, full_name, email)
{% endset %}
{% do run_query(create_src) %}

{% set cases = [] %}

-- defaults (X / x / n, other characters kept)
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_mask_src'],
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
{% do cases.append({"name": "defaults (X / x / n, other characters kept)", "expect": {"full_name_masked": ["Xxxx Xxx nn", none]}, "sql": macro_query}) %}

-- NULL keeps upper case, custom digit substitute
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_mask_src'],
    column_names=['full_name'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='NULL',
    lower_char_substitute='',
    digit_char_substitute='#',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_masked',
    combined_hash_column_name=''
) }}) AS masked ORDER BY id
{% endset %}
{% do cases.append({"name": "NULL keeps upper case, custom digit substitute", "expect": {"full_name_masked": ["Jxxx Dxx ##", none]}, "sql": macro_query}) %}

-- all categories NULL leaves the value unchanged
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_mask_src'],
    column_names=['full_name'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='NULL',
    lower_char_substitute='NULL',
    digit_char_substitute='NULL',
    other_char_substitute='NULL',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_masked',
    combined_hash_column_name=''
) }}) AS masked ORDER BY id
{% endset %}
{% do cases.append({"name": "all categories NULL leaves the value unchanged", "expect": {"full_name_masked": ["John Doe 42", none]}, "sql": macro_query}) %}

-- other-character substitute
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_mask_src'],
    column_names=['email'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='_',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_masked',
    combined_hash_column_name=''
) }}) AS masked ORDER BY id
{% endset %}
{% do cases.append({"name": "other-character substitute", "expect": {"email_masked": ["xx_x_xx", none]}, "sql": macro_query}) %}

-- other-character substitute also replaces spaces, like Databricks mask()
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_mask_src'],
    column_names=['full_name'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='_',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_masked',
    combined_hash_column_name=''
) }}) AS masked ORDER BY id
{% endset %}
{% do cases.append({"name": "other-character substitute also replaces spaces, like Databricks mask()", "expect": {"full_name_masked": ["Xxxx_Xxx_nn", none]}, "sql": macro_query}) %}

-- multiple columns
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_mask_src'],
    column_names=['full_name', 'email'],
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
{% do cases.append({"name": "multiple columns", "expect": {"full_name_masked": ["Xxxx Xxx nn", none], "email_masked": ["xx@x.xx", none]}, "sql": macro_query}) %}

{% for case in cases %}
    {% set results = run_query(case.sql) %}
    {% if case.columns is defined and (results.column_names | list) != case.columns %}
        {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | list %}
        {% if expected == "ref" %}
            {# integer hash: compare with the database's own function called on the source #}
            {% set expected = run_query("SELECT " ~ case.ref ~ " FROM data_masking_mask_src ORDER BY id").columns[0].values() | list %}
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
