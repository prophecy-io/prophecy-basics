-- Test: DataMasking macro - one-way hash methods (Snowflake)
-- Validates every hash method the gem offers on Snowflake.
-- Runs on Snowflake; the snowflake__ adapter casts to VARCHAR and calls SHA1 / SHA2 / MD5 / HASH.
--
-- Expected digests come from Python's hashlib, so a failure means the macro
-- no longer produces the standard hex digest of the input value.
--
-- Every call passes remaining_columns='' so it round-trips through the
-- code-to-visual tester, which has no input schema to rebuild that list from.
-- Value checks therefore add a "_masked" suffix column next to the originals.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE DATA_MASKING_HASH_SRC AS
SELECT * FROM (VALUES (1, 'John Doe 42', 'jd@x.io', 42), (2, NULL, NULL, NULL)) AS T(ID, FULL_NAME, EMAIL, NUM)
{% endset %}
{% do run_query(create_src) %}

{% set cases = [] %}

-- sha (SHA-1)
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='sha',
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
{% do cases.append({"name": "sha (SHA-1)", "expect": {"FULL_NAME_MASKED": ["f36756611043e467f84cb9e2b7a6e299cf088e1a", none]}, "sql": macro_query}) %}

-- sha2 224
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='sha2',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='224',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name=''
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "sha2 224", "expect": {"FULL_NAME_MASKED": ["61e693ccdf3b1cdc99b5f542a326164a4c9b625a3a019bd23e718bfe", none]}, "sql": macro_query}) %}

-- sha2 256
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='sha2',
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
{% do cases.append({"name": "sha2 256", "expect": {"FULL_NAME_MASKED": ["2e20fe7a0aada5cfec1cdfaa3de15ff005cd74d39e217d14f459199811ca7abf", none]}, "sql": macro_query}) %}

-- sha2 384
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='sha2',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='384',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name=''
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "sha2 384", "expect": {"FULL_NAME_MASKED": ["4c9cb6f4da50b4c7e8eba6c3091c04bcb16ff3a1ed662ae8d98e6ff9fbd2f998bcd15a11edceb3d247e2ec15d81b5c88", none]}, "sql": macro_query}) %}

-- sha2 512
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='sha2',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='512',
    masked_column_add_method='prefix_suffix_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name=''
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "sha2 512", "expect": {"FULL_NAME_MASKED": ["dd0eca907cf7d5e862d2cb32c27c2c4760f3109c2d555dfa3875cc60bb7d50c11813891997f20f1e4f7cbb4eedd796946731e6e1532b09d34e5ec12768f1eb5f", none]}, "sql": macro_query}) %}

-- md5
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='md5',
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
{% do cases.append({"name": "md5", "expect": {"FULL_NAME_MASKED": ["3bd2e7d3239609f8266391d5c729207f", none]}, "sql": macro_query}) %}

-- sha2 on a non-string column hashes its text form
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['NUM'],
    remaining_columns='',
    masking_method='sha2',
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
{% do cases.append({"name": "sha2 on a non-string column hashes its text form", "expect": {"NUM_MASKED": ["73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049", none]}, "sql": macro_query}) %}

-- md5 on multiple columns
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME', 'EMAIL'],
    remaining_columns='',
    masking_method='md5',
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
{% do cases.append({"name": "md5 on multiple columns", "expect": {"FULL_NAME_MASKED": ["3bd2e7d3239609f8266391d5c729207f", none], "EMAIL_MASKED": ["e194413f0765a1c32d46cbbb10e88f87", none]}, "sql": macro_query}) %}

-- hash returns the same integer as calling the hash function directly
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME'],
    remaining_columns='',
    masking_method='hash',
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
{% do cases.append({"name": "hash returns the same integer as calling the hash function directly", "expect": {"FULL_NAME_MASKED": "ref"}, "ref": "HASH(FULL_NAME)", "sql": macro_query}) %}

-- combined hash over several columns
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.DataMasking(
    relation_name=['DATA_MASKING_HASH_SRC'],
    column_names=['FULL_NAME', 'EMAIL'],
    remaining_columns='',
    masking_method='hash',
    upper_char_substitute='',
    lower_char_substitute='',
    digit_char_substitute='',
    other_char_substitute='',
    sha2_bit_length='256',
    masked_column_add_method='combinedHash_substitute',
    prefix_suffix_opt='Suffix',
    prefix_suffix_val='_MASKED',
    combined_hash_column_name='ROW_HASH'
) }}) AS masked ORDER BY ID
{% endset %}
{% do cases.append({"name": "combined hash over several columns", "expect": {"ROW_HASH": "ref"}, "ref": "HASH(FULL_NAME, EMAIL)", "sql": macro_query}) %}

{% for case in cases %}
    {% set results = run_query(case.sql) %}
    {% if case.columns is defined and (results.column_names | list) != case.columns %}
        {{ exceptions.raise_compiler_error("DataMasking test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | list %}
        {% if expected == "ref" %}
            {# integer hash: compare with the database's own function called on the source #}
            {% set expected = run_query("SELECT " ~ case.ref ~ " FROM DATA_MASKING_HASH_SRC ORDER BY ID").columns[0].values() | list %}
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
