-- Test: DataMasking macro - one-way hash methods (Databricks)
-- Validates every hash method the gem offers on Databricks.
-- Runs on Databricks; the default__ adapter calls sha / sha2 / md5 / hash / crc32.
--
-- Expected digests come from Python's hashlib, so a failure means the macro
-- no longer produces the standard hex digest of the input value.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY VIEW data_masking_hash_src AS
SELECT * FROM VALUES (1, 'John Doe 42', 'jd@x.io', 42), (2, NULL, NULL, NULL) AS t(id, full_name, email, num)
{% endset %}
{% do run_query(create_src) %}

{% set base_args = {"relation_name": ["data_masking_hash_src"], "column_names": ["full_name"], "remaining_columns": "id, email, num", "masking_method": "sha2", "upper_char_substitute": "", "lower_char_substitute": "", "digit_char_substitute": "", "other_char_substitute": "", "sha2_bit_length": "256", "masked_column_add_method": "inplace_substitute", "prefix_suffix_opt": "Prefix", "prefix_suffix_val": "", "combined_hash_column_name": ""} %}

{% set cases = [
    {"name": "sha (SHA-1)", "args": {"masking_method": "sha"}, "expect": {"full_name": ["f36756611043e467f84cb9e2b7a6e299cf088e1a", none]}},
    {"name": "sha2 224", "args": {"sha2_bit_length": "224"}, "expect": {"full_name": ["61e693ccdf3b1cdc99b5f542a326164a4c9b625a3a019bd23e718bfe", none]}},
    {"name": "sha2 256", "args": {"sha2_bit_length": "256"}, "expect": {"full_name": ["2e20fe7a0aada5cfec1cdfaa3de15ff005cd74d39e217d14f459199811ca7abf", none]}},
    {"name": "sha2 384", "args": {"sha2_bit_length": "384"}, "expect": {"full_name": ["4c9cb6f4da50b4c7e8eba6c3091c04bcb16ff3a1ed662ae8d98e6ff9fbd2f998bcd15a11edceb3d247e2ec15d81b5c88", none]}},
    {"name": "sha2 512", "args": {"sha2_bit_length": "512"}, "expect": {"full_name": ["dd0eca907cf7d5e862d2cb32c27c2c4760f3109c2d555dfa3875cc60bb7d50c11813891997f20f1e4f7cbb4eedd796946731e6e1532b09d34e5ec12768f1eb5f", none]}},
    {"name": "md5", "args": {"masking_method": "md5"}, "expect": {"full_name": ["3bd2e7d3239609f8266391d5c729207f", none]}},
    {"name": "crc32", "args": {"masking_method": "crc32"}, "expect": {"full_name": [1510493331, none]}},
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

-- hash returns an integer, so compare against hash() called directly on the source:
-- this checks the right columns are hashed and that the result is not a hex string.
{% set hash_cases = [
    {"name": "hash in place", "method": "inplace_substitute", "columns": ["full_name"], "out": "full_name", "ref": "hash(full_name)"},
    {"name": "hash combined", "method": "combinedHash_substitute", "columns": ["full_name", "email"], "out": "row_hash", "ref": "hash(full_name, email)"}
] %}
{% for case in hash_cases %}
    {% set args = dict(base_args, masking_method="hash", masked_column_add_method=case.method, column_names=case.columns, combined_hash_column_name="row_hash") %}
    {% set macro_query %}
    SELECT * FROM ({{ prophecy_basics.DataMasking(**args) }}) AS masked ORDER BY id
    {% endset %}
    {% set actual = run_query(macro_query).columns[case.out].values() | list %}
    {% set expected = run_query("SELECT " ~ case.ref ~ " AS h FROM data_masking_hash_src ORDER BY id").columns[0].values() | list %}
    {% if actual != expected or actual[0] is none %}
        {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED: expected " ~ expected ~ ", got " ~ actual) }}
    {% endif %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
