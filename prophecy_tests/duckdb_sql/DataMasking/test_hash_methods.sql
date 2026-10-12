-- Test: DataMasking macro - one-way hash methods (DuckDB)
-- Validates every hash method the gem offers on DuckDB.
-- Runs on DuckDB; the duckdb__ adapter casts to VARCHAR and calls sha1 / sha256 / md5.
-- DuckDB only supports SHA2 with 256 bits, so 224/384/512 are not covered here.
--
-- Expected digests come from Python's hashlib, so a failure means the macro
-- no longer produces the standard hex digest of the input value.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE data_masking_hash_src AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io', 42), (2, NULL, NULL, NULL)) AS t(id, full_name, email, num)
{% endset %}
{% do run_query(create_src) %}

{% set base_args = {"relation_name": ["data_masking_hash_src"], "column_names": ["full_name"], "remaining_columns": "id, email, num", "masking_method": "sha2", "upper_char_substitute": "", "lower_char_substitute": "", "digit_char_substitute": "", "other_char_substitute": "", "sha2_bit_length": "256", "masked_column_add_method": "inplace_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "", "combined_hash_column_name": ""} %}

{% set cases = [
    {"name": "sha (SHA-1)", "args": {"masking_method": "sha"}, "expect": {"full_name": ["f36756611043e467f84cb9e2b7a6e299cf088e1a", none]}},
    {"name": "sha2 256", "args": {"sha2_bit_length": "256"}, "expect": {"full_name": ["2e20fe7a0aada5cfec1cdfaa3de15ff005cd74d39e217d14f459199811ca7abf", none]}},
    {"name": "md5", "args": {"masking_method": "md5"}, "expect": {"full_name": ["3bd2e7d3239609f8266391d5c729207f", none]}},
    {"name": "sha2 on a non-string column hashes its text form", "args": {"column_names": ["num"], "remaining_columns": "id, full_name, email"}, "expect": {"num": ["73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049", none]}},
    {"name": "md5 on multiple columns", "args": {"masking_method": "md5", "column_names": ["full_name", "email"], "remaining_columns": "id, num"}, "expect": {"full_name": ["3bd2e7d3239609f8266391d5c729207f", none], "email": ["e194413f0765a1c32d46cbbb10e88f87", none]}}
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
