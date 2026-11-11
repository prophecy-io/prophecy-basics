-- Test: DataValidation macro - what happens when checks fail (Snowflake)
-- Each case embeds the macro in a CTE, the way Prophecy-generated models call it, then
-- compares the output column list and the ordered values (as text) with the expected result.
-- The schema and columns use upper-case names, as Prophecy reports them for unquoted
-- Snowflake columns; check names and result columns follow that case.
--
--   summary: one row per check, including the table-level row count and required columns checks
--   fail:    clean data flows through unchanged (a failing run stops the query, which a dbt
--            test cannot catch, so that path is covered in the README instead)

{% if execute %}
{% set cases = [] %}

{% set shipments_cte %}
WITH shipments AS (
    SELECT 'SH-1001' AS SHIPMENT_ID, 'DE' AS COUNTRY, 12 AS WEIGHT_KG
    UNION ALL SELECT 'SH-1002', 'FR', 4
    UNION ALL SELECT 'SH-1003', 'XX', 7
)
{% endset %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'required_columns', 'columns': ['SHIPMENT_ID', 'WEIGHT_KG']}, {'checkType': 'row_count', 'minValue': '5'}, {'checkType': 'regex_match', 'columns': ['SHIPMENT_ID'], 'pattern': 'SH-[0-9]{4}'}, {'checkType': 'value_range', 'columns': ['WEIGHT_KG'], 'maxValue': '10', 'checkName': 'Parcel weight limit'}], 'summary', 'continue') }}
)
SELECT * FROM validated ORDER BY CHECK_NUMBER
{% endset %}
{% do cases.append({"name": "summary: one row per check", "columns": ["CHECK_NUMBER", "CHECK_NAME", "CHECK_TYPE", "FAILED_ROWS", "CHECK_PASSED"], "expect": {"CHECK_NAME": ["Required columns: SHIPMENT_ID, WEIGHT_KG", "Row count", "Pattern match: SHIPMENT_ID", "Parcel weight limit"], "CHECK_TYPE": ["required_columns", "row_count", "regex_match", "value_range"], "FAILED_ROWS": ["0", "3", "0", "1"], "CHECK_PASSED": ["True", "False", "True", "False"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'not_null', 'columns': ['SHIPMENT_ID', 'COUNTRY']}, {'checkType': 'unique', 'columns': ['SHIPMENT_ID']}, {'checkType': 'row_count', 'minValue': '1', 'maxValue': '1000'}], 'passed', 'fail') }}
)
SELECT * FROM validated ORDER BY SHIPMENT_ID
{% endset %}
{% do cases.append({"name": "fail: clean data flows through unchanged", "columns": ["SHIPMENT_ID", "COUNTRY", "WEIGHT_KG"], "expect": {"SHIPMENT_ID": ["SH-1001", "SH-1002", "SH-1003"]}, "sql": macro_query}) %}

{% for case in cases %}
    {% set results = run_query(case.sql) %}
    {% if (results.column_names | list) != case.columns %}
        {{ exceptions.raise_compiler_error("DataValidation test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | map('string') | list %}
        {% if actual != expected %}
            {{ exceptions.raise_compiler_error("DataValidation test '" ~ case.name ~ "' FAILED on column " ~ col ~ ": expected " ~ expected ~ ", got " ~ actual) }}
        {% endif %}
    {% endfor %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
