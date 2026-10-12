-- Test: DataMasking macro - output column layout (Snowflake)
-- Validates which columns come back, and in what order, for each way of adding
-- masked columns: replace in place, add with a prefix or suffix, combined hash.
-- Runs on Snowflake.
--
-- In place: the unmasked columns come first (in the order the gem passes them),
-- followed by the masked columns under their original names.
-- Prefix/suffix and combined hash: every original column is kept unchanged and
-- the new columns are appended.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE DATA_MASKING_COLUMNS_SRC AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io')) AS T(ID, FULL_NAME, EMAIL)
{% endset %}
{% do run_query(create_src) %}

{% set base_args = {"relation_name": ["DATA_MASKING_COLUMNS_SRC"], "column_names": ["FULL_NAME"], "remaining_columns": "ID, EMAIL", "masking_method": "md5", "upper_char_substitute": "", "lower_char_substitute": "", "digit_char_substitute": "", "other_char_substitute": "", "sha2_bit_length": "256", "masked_column_add_method": "inplace_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "", "combined_hash_column_name": ""} %}

{% set cases = [
    {"name": "in place", "args": {}, "columns": ["ID", "EMAIL", "FULL_NAME"], "expect": {"FULL_NAME": ["3bd2e7d3239609f8266391d5c729207f"]}},
    {"name": "in place with no other columns", "args": {"column_names": ["ID", "FULL_NAME", "EMAIL"], "remaining_columns": ""}, "columns": ["ID", "FULL_NAME", "EMAIL"], "expect": {"FULL_NAME": ["3bd2e7d3239609f8266391d5c729207f"]}},
    {"name": "prefix", "args": {"masked_column_add_method": "prefix_suffix_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "MASKED_"}, "columns": ["ID", "FULL_NAME", "EMAIL", "MASKED_FULL_NAME"], "expect": {"FULL_NAME": ["John Doe 42"], "MASKED_FULL_NAME": ["3bd2e7d3239609f8266391d5c729207f"]}},
    {"name": "suffix", "args": {"masked_column_add_method": "prefix_suffix_substitute", "prefix_suffix_opt": "Suffix", "prefix_suffix_val": "_MASKED", "masking_method": "mask"}, "columns": ["ID", "FULL_NAME", "EMAIL", "FULL_NAME_MASKED"], "expect": {"FULL_NAME": ["John Doe 42"], "FULL_NAME_MASKED": ["Xxxx Xxx nn"]}},
    {"name": "combined hash keeps every column and appends one", "args": {"masking_method": "hash", "masked_column_add_method": "combinedHash_substitute", "column_names": ["FULL_NAME", "EMAIL"], "combined_hash_column_name": "ROW_HASH"}, "columns": ["ID", "FULL_NAME", "EMAIL", "ROW_HASH"], "expect": {"FULL_NAME": ["John Doe 42"]}}
] %}

{% for case in cases %}
    {% set args = dict(base_args, **case.args) %}
    {% set macro_query %}
    SELECT * FROM ({{ prophecy_basics.DataMasking(**args) }}) AS masked ORDER BY ID
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
