-- Test: DataMasking macro - one-way hash methods (DuckDB)
-- Validates every hash method the gem offers on DuckDB.
-- Runs on DuckDB; the duckdb__ adapter casts to VARCHAR and calls sha1 / sha256 / md5.
-- DuckDB only supports SHA2 with 256 bits, so 224/384/512 are not covered here.
--
-- Expected digests come from Python's hashlib, so a failure means the macro
-- no longer produces the standard hex digest of the input value.
--
-- Every call passes remaining_columns='' so it round-trips through the
-- code-to-visual tester, which has no input schema to rebuild that list from.
-- Value checks therefore add a "_masked" suffix column next to the originals.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE data_masking_hash_src AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io', 42), (2, NULL, NULL, NULL)) AS t(id, full_name, email, num)
{% endset %}
{% do run_query(create_src) %}

{% set cases = [] %}

-- sha (SHA-1)
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_hash_src'],
    column_names=['full_name'],
    remaining_columns='',
    masking_method='sha',
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
{% do cases.append({"name": "sha (SHA-1)", "expect": {"full_name_masked": ["f36756611043e467f84cb9e2b7a6e299cf088e1a", none]}, "sql": macro_query}) %}

-- sha2 256
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_hash_src'],
    column_names=['full_name'],
    remaining_columns='',
    masking_method='sha2',
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
{% do cases.append({"name": "sha2 256", "expect": {"full_name_masked": ["2e20fe7a0aada5cfec1cdfaa3de15ff005cd74d39e217d14f459199811ca7abf", none]}, "sql": macro_query}) %}

-- md5
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_hash_src'],
    column_names=['full_name'],
    remaining_columns='',
    masking_method='md5',
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
{% do cases.append({"name": "md5", "expect": {"full_name_masked": ["3bd2e7d3239609f8266391d5c729207f", none]}, "sql": macro_query}) %}

-- sha2 on a non-string column hashes its text form
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_hash_src'],
    column_names=['num'],
    remaining_columns='',
    masking_method='sha2',
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
{% do cases.append({"name": "sha2 on a non-string column hashes its text form", "expect": {"num_masked": ["73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049", none]}, "sql": macro_query}) %}

-- md5 on multiple columns
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['data_masking_hash_src'],
    column_names=['full_name', 'email'],
    remaining_columns='',
    masking_method='md5',
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
{% do cases.append({"name": "md5 on multiple columns", "expect": {"full_name_masked": ["3bd2e7d3239609f8266391d5c729207f", none], "email_masked": ["e194413f0765a1c32d46cbbb10e88f87", none]}, "sql": macro_query}) %}

{% for case in cases %}
    {% set results = run_query(case.sql) %}
    {% if case.columns is defined and (results.column_names | list) != case.columns %}
        {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | list %}
        {% if expected == "ref" %}
            {# integer hash: compare with the database's own function called on the source #}
            {% set expected = run_query("SELECT " ~ case.ref ~ " FROM data_masking_hash_src ORDER BY id").columns[0].values() | list %}
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
