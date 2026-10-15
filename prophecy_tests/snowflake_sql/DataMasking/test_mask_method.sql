-- Test: DataMasking macro - mask method (Snowflake)
-- Validates character masking: default substitutes, "NULL" to keep a category,
-- custom substitutes, other-character masking and NULL passthrough.
-- Runs on Snowflake; the snowflake__ adapter builds a REGEXP_REPLACE chain.
-- Unlike Databricks mask(), the Snowflake other-character pattern keeps whitespace,
-- so the other-character case uses a value without spaces.
--
-- Each case runs the macro on a two-row source (row 2 is all NULL) and compares
-- every listed column, ordered by id, against the expected values.
--
-- Every call passes remaining_columns='' so it round-trips through the
-- code-to-visual tester, which has no input schema to rebuild that list from.
-- Value checks therefore add a "_masked" suffix column next to the originals.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE DATA_MASKING_MASK_SRC AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io'), (2, NULL, NULL)) AS T(ID, FULL_NAME, EMAIL)
{% endset %}
{% do run_query(create_src) %}

{% set cases = [] %}

-- defaults (X / x / n, other characters kept)
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_MASK_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name=''
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "defaults (X / x / n, other characters kept)", "expect": {"FULL_NAME_MASKED": ["Xxxx Xxx nn", none]}, "sql": macro_query}) %}

-- NULL keeps upper case, custom digit substitute
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_MASK_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='NULL',
    lower_char_substitute='',
    digit_char_substitute='#',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name=''
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "NULL keeps upper case, custom digit substitute", "expect": {"FULL_NAME_MASKED": ["Jxxx Dxx ##", none]}, "sql": macro_query}) %}

-- all categories NULL leaves the value unchanged
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_MASK_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='NULL',
    lower_char_substitute='NULL',
    digit_char_substitute='NULL',
    other_char_substitute='NULL',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name=''
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "all categories NULL leaves the value unchanged", "expect": {"FULL_NAME_MASKED": ["John Doe 42", none]}, "sql": macro_query}) %}

-- other-character substitute
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_MASK_SRC'],
    column_names=['EMAIL'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='_',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name=''
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "other-character substitute", "expect": {"EMAIL_MASKED": ["xx_x_xx", none]}, "sql": macro_query}) %}

-- multiple columns
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_MASK_SRC'],
    column_names=['FULL_NAME', 'EMAIL'],
    remaining_columns='',
    masking_method='mask',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name=''
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "multiple columns", "expect": {"FULL_NAME_MASKED": ["Xxxx Xxx nn", none], "EMAIL_MASKED": ["xx@x.xx", none]}, "sql": macro_query}) %}

{% for case in cases %}
    {% set results = run_query(case.sql) %}
    {% if case.columns is defined and (results.column_names | list) != case.columns %}
        {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | list %}
        {% if expected == "ref" %}
            {# integer hash: compare with the database's own function called on the source #}
            {% set expected = run_query("SELECT " ~ case.ref ~ " FROM DATA_MASKING_MASK_SRC ORDER BY ID").columns[0].values() | list %}
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
