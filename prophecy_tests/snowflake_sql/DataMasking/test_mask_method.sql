-- Test: DataMasking macro - mask method (Snowflake)
-- Validates character masking: default substitutes, "NULL" to keep a category,
-- custom substitutes, other-character masking and NULL passthrough.
-- Runs on Snowflake; the snowflake__ adapter builds a REGEXP_REPLACE chain.
-- Unlike Databricks mask(), the Snowflake other-character pattern keeps whitespace,
-- so the other-character case uses a value without spaces.
--
-- Each case runs the macro on a two-row source (row 2 is all NULL) and compares
-- every listed column, ordered by id, against the expected values.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE DATA_MASKING_MASK_SRC AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io'), (2, NULL, NULL)) AS T(ID, FULL_NAME, EMAIL)
{% endset %}
{% do run_query(create_src) %}

{% set base_args = {"relation_name": ["DATA_MASKING_MASK_SRC"], "column_names": ["FULL_NAME"], "remaining_columns": "ID, EMAIL", "masking_method": "mask", "upper_char_substitute": "", "lower_char_substitute": "", "digit_char_substitute": "", "other_char_substitute": "", "sha2_bit_length": "256", "masked_column_add_method": "inplace_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "", "combined_hash_column_name": ""} %}

{% set cases = [
    {"name": "defaults (X / x / n, other characters kept)", "args": {}, "expect": {"FULL_NAME": ["Xxxx Xxx nn", none]}},
    {"name": "NULL keeps upper case, custom digit substitute", "args": {"upper_char_substitute": "NULL", "digit_char_substitute": "#"}, "expect": {"FULL_NAME": ["Jxxx Dxx ##", none]}},
    {"name": "all categories NULL leaves the value unchanged", "args": {"upper_char_substitute": "NULL", "lower_char_substitute": "NULL", "digit_char_substitute": "NULL", "other_char_substitute": "NULL"}, "expect": {"FULL_NAME": ["John Doe 42", none]}},
    {"name": "other-character substitute", "args": {"column_names": ["EMAIL"], "remaining_columns": "ID, FULL_NAME", "other_char_substitute": "_"}, "expect": {"EMAIL": ["xx_x_xx", none]}},
    {"name": "multiple columns", "args": {"column_names": ["FULL_NAME", "EMAIL"], "remaining_columns": "ID"}, "expect": {"FULL_NAME": ["Xxxx Xxx nn", none], "EMAIL": ["xx@x.xx", none]}}
] %}

{% for case in cases %}
    {% set args = dict(base_args, **case.args) %}
    {% set macro_query %}
    SELECT * FROM ({{ prophecy_basics.DataMasking(**args) }}) AS masked ORDER BY ID
    {% endset %}
    {% set results = run_query(macro_query) %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | list %}
        {% if actual != expected %}
            {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED on column " ~ col ~ ": expected " ~ expected ~ ", got " ~ actual) }}
        {% endif %}
    {% endfor %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
