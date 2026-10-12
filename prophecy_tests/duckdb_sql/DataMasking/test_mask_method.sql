-- Test: DataMasking macro - mask method (DuckDB)
-- Validates character masking: default substitutes, "NULL" to keep a category,
-- custom substitutes, other-character masking and NULL passthrough.
-- Runs on DuckDB; the duckdb__ adapter builds a regexp_replace chain.
--
-- Each case runs the macro on a two-row source (row 2 is all NULL) and compares
-- every listed column, ordered by id, against the expected values.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE data_masking_mask_src AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io'), (2, NULL, NULL)) AS t(id, full_name, email)
{% endset %}
{% do run_query(create_src) %}

{% set base_args = {"relation_name": ["data_masking_mask_src"], "column_names": ["full_name"], "remaining_columns": "id, email", "masking_method": "mask", "upper_char_substitute": "", "lower_char_substitute": "", "digit_char_substitute": "", "other_char_substitute": "", "sha2_bit_length": "256", "masked_column_add_method": "inplace_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "", "combined_hash_column_name": ""} %}

{% set cases = [
    {"name": "defaults (X / x / n, other characters kept)", "args": {}, "expect": {"full_name": ["Xxxx Xxx nn", none]}},
    {"name": "NULL keeps upper case, custom digit substitute", "args": {"upper_char_substitute": "NULL", "digit_char_substitute": "#"}, "expect": {"full_name": ["Jxxx Dxx ##", none]}},
    {"name": "all categories NULL leaves the value unchanged", "args": {"upper_char_substitute": "NULL", "lower_char_substitute": "NULL", "digit_char_substitute": "NULL", "other_char_substitute": "NULL"}, "expect": {"full_name": ["John Doe 42", none]}},
    {"name": "other-character substitute", "args": {"column_names": ["email"], "remaining_columns": "id, full_name", "other_char_substitute": "_"}, "expect": {"email": ["xx_x_xx", none]}},
    {"name": "other-character substitute also replaces spaces, like Databricks mask()", "args": {"other_char_substitute": "_"}, "expect": {"full_name": ["Xxxx_Xxx_nn", none]}},
    {"name": "multiple columns", "args": {"column_names": ["full_name", "email"], "remaining_columns": "id"}, "expect": {"full_name": ["Xxxx Xxx nn", none], "email": ["xx@x.xx", none]}}
] %}

{% for case in cases %}
    {% set args = dict(base_args, **case.args) %}
    {% set macro_query %}
    SELECT * FROM ({{ prophecy_basics.DataMasking(**args) }}) AS masked ORDER BY id
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
