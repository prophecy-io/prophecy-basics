-- Test: DataMasking macro - one-way hash methods (Snowflake)
-- Validates every hash method the gem offers on Snowflake.
-- Runs on Snowflake; the snowflake__ adapter casts to VARCHAR and calls SHA1 / SHA2 / MD5 / HASH.
--
-- Expected digests come from Python's hashlib, so a failure means the macro
-- no longer produces the standard hex digest of the input value.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE DATA_MASKING_HASH_SRC AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io', 42), (2, NULL, NULL, NULL)) AS T(ID, FULL_NAME, EMAIL, NUM)
{% endset %}
{% do run_query(create_src) %}

{% set base_args = {"relation_name": ["DATA_MASKING_HASH_SRC"], "column_names": ["FULL_NAME"], "remaining_columns": "ID, EMAIL, NUM", "masking_method": "sha2", "upper_char_substitute": "", "lower_char_substitute": "", "digit_char_substitute": "", "other_char_substitute": "", "sha2_bit_length": "256", "masked_column_add_method": "inplace_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "", "combined_hash_column_name": ""} %}

{% set cases = [
    {"name": "sha (SHA-1)", "args": {"masking_method": "sha"}, "expect": {"FULL_NAME": ["f36756611043e467f84cb9e2b7a6e299cf088e1a", none]}},
    {"name": "sha2 224", "args": {"sha2_bit_length": "224"}, "expect": {"FULL_NAME": ["61e693ccdf3b1cdc99b5f542a326164a4c9b625a3a019bd23e718bfe", none]}},
    {"name": "sha2 256", "args": {"sha2_bit_length": "256"}, "expect": {"FULL_NAME": ["2e20fe7a0aada5cfec1cdfaa3de15ff005cd74d39e217d14f459199811ca7abf", none]}},
    {"name": "sha2 384", "args": {"sha2_bit_length": "384"}, "expect": {"FULL_NAME": ["4c9cb6f4da50b4c7e8eba6c3091c04bcb16ff3a1ed662ae8d98e6ff9fbd2f998bcd15a11edceb3d247e2ec15d81b5c88", none]}},
    {"name": "sha2 512", "args": {"sha2_bit_length": "512"}, "expect": {"FULL_NAME": ["dd0eca907cf7d5e862d2cb32c27c2c4760f3109c2d555dfa3875cc60bb7d50c11813891997f20f1e4f7cbb4eedd796946731e6e1532b09d34e5ec12768f1eb5f", none]}},
    {"name": "md5", "args": {"masking_method": "md5"}, "expect": {"FULL_NAME": ["3bd2e7d3239609f8266391d5c729207f", none]}},
    {"name": "sha2 on a non-string column hashes its text form", "args": {"column_names": ["NUM"], "remaining_columns": "ID, FULL_NAME, EMAIL"}, "expect": {"NUM": ["73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049", none]}},
    {"name": "md5 on multiple columns", "args": {"masking_method": "md5", "column_names": ["FULL_NAME", "EMAIL"], "remaining_columns": "ID, NUM"}, "expect": {"FULL_NAME": ["3bd2e7d3239609f8266391d5c729207f", none], "EMAIL": ["e194413f0765a1c32d46cbbb10e88f87", none]}}
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

-- hash returns an integer, so compare against HASH() called directly on the source:
-- this checks the right columns are hashed and that the result is not a hex string.
{% set hash_cases = [
    {"name": "hash in place", "method": "inplace_substitute", "columns": ["FULL_NAME"], "out": "FULL_NAME", "ref": "HASH(FULL_NAME)"},
    {"name": "hash combined", "method": "combinedHash_substitute", "columns": ["FULL_NAME", "EMAIL"], "out": "ROW_HASH", "ref": "HASH(FULL_NAME, EMAIL)"}
] %}
{% for case in hash_cases %}
    {% set args = dict(base_args, masking_method="hash", masked_column_add_method=case.method, column_names=case.columns, combined_hash_column_name="ROW_HASH") %}
    {% set macro_query %}
    SELECT * FROM ({{ prophecy_basics.DataMasking(**args) }}) AS masked ORDER BY ID
    {% endset %}
    {% set actual = run_query(macro_query).columns[case.out].values() | list %}
    {% set expected = run_query("SELECT " ~ case.ref ~ " AS h FROM DATA_MASKING_HASH_SRC ORDER BY ID").columns[0].values() | list %}
    {% if actual != expected or actual[0] is none %}
        {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED: expected " ~ expected ~ ", got " ~ actual) }}
    {% endif %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
