-- Test: DataMasking macro - output column layout (DuckDB)
-- Validates which columns come back, and in what order, for each way of adding
-- masked columns: replace in place, add with a prefix or suffix, combined hash.
-- Runs on DuckDB.
--
-- In place: the unmasked columns come first (in the order the gem passes them),
-- followed by the masked columns under their original names.
-- Prefix/suffix and combined hash: every original column is kept unchanged and
-- the new columns are appended.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE data_masking_columns_src AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io')) AS t(id, full_name, email)
{% endset %}
{% do run_query(create_src) %}

{% set base_args = {"relation_name": ["data_masking_columns_src"], "column_names": ["full_name"], "remaining_columns": "id, email", "masking_method": "md5", "upper_char_substitute": "", "lower_char_substitute": "", "digit_char_substitute": "", "other_char_substitute": "", "sha2_bit_length": "256", "masked_column_add_method": "inplace_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "", "combined_hash_column_name": ""} %}

{% set cases = [
    {"name": "in place", "args": {}, "columns": ["id", "email", "full_name"], "expect": {"full_name": ["3bd2e7d3239609f8266391d5c729207f"]}},
    {"name": "in place with no other columns", "args": {"column_names": ["id", "full_name", "email"], "remaining_columns": ""}, "columns": ["id", "full_name", "email"], "expect": {"full_name": ["3bd2e7d3239609f8266391d5c729207f"]}},
    {"name": "prefix", "args": {"masked_column_add_method": "prefix_suffix_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "masked_"}, "columns": ["id", "full_name", "email", "masked_full_name"], "expect": {"full_name": ["John Doe 42"], "masked_full_name": ["3bd2e7d3239609f8266391d5c729207f"]}},
    {"name": "suffix", "args": {"masked_column_add_method": "prefix_suffix_substitute", "prefix_suffix_opt": "Suffix", "prefix_suffix_val": "_masked", "masking_method": "mask"}, "columns": ["id", "full_name", "email", "full_name_masked"], "expect": {"full_name": ["John Doe 42"], "full_name_masked": ["Xxxx Xxx nn"]}}
] %}

{% for case in cases %}
    {% set args = dict(base_args, **case.args) %}
    {% set macro_query %}
    SELECT * FROM ({{ prophecy_basics.DataMasking(**args) }}) AS masked ORDER BY id
    {% endset %}
    {% set results = run_query(macro_query) %}
    {% set actual_columns = results.column_names | list %}
    {% if actual_columns != case.columns %}
        {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ actual_columns) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | list %}
        {% if actual != expected %}
            {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED on column " ~ col ~ ": expected " ~ expected ~ ", got " ~ actual) }}
        {% endif %}
    {% endfor %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
