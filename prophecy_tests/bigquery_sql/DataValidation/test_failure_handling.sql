-- Test: DataValidation macro - what happens when checks fail (BigQuery)
-- Each case embeds the macro in a CTE, the way Prophecy-generated models call it, then
-- compares the output column list and the ordered values (as text) with the expected result.
--
--   summary: one row per check, including the table-level row count and required columns checks
--   fail:    clean data flows through unchanged (a failing run stops the query, which a dbt
--            test cannot catch, so that path is covered in the README instead)

{% if execute %}
{% set cases = [] %}

{% set shipments_cte %}
WITH shipments AS (
    SELECT 'SH-1001' AS shipment_id, 'DE' AS country, 12 AS weight_kg
    UNION ALL SELECT 'SH-1002', 'FR', 4
    UNION ALL SELECT 'SH-1003', 'XX', 7
)
{% endset %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'required_columns', 'columns': ['shipment_id', 'weight_kg']}, {'checkType': 'row_count', 'minValue': '5'}, {'checkType': 'regex_match', 'columns': ['shipment_id'], 'pattern': 'SH-[0-9]{4}'}, {'checkType': 'value_range', 'columns': ['weight_kg'], 'maxValue': '10', 'checkName': 'Parcel weight limit'}], 'summary', 'continue') }}
)
SELECT * FROM validated ORDER BY check_number
{% endset %}
{% do cases.append({"name": "summary: one row per check", "columns": ["check_number", "check_name", "check_type", "failed_rows", "check_passed"], "expect": {"check_name": ["Required columns: shipment_id, weight_kg", "Row count", "Pattern match: shipment_id", "Parcel weight limit"], "check_type": ["required_columns", "row_count", "regex_match", "value_range"], "failed_rows": ["0", "3", "0", "1"], "check_passed": ["True", "False", "True", "False"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'not_null', 'columns': ['shipment_id', 'country']}, {'checkType': 'unique', 'columns': ['shipment_id']}, {'checkType': 'row_count', 'minValue': '1', 'maxValue': '1000'}], 'passed', 'fail') }}
)
SELECT * FROM validated ORDER BY shipment_id
{% endset %}
{% do cases.append({"name": "fail: clean data flows through unchanged", "columns": ["shipment_id", "country", "weight_kg"], "expect": {"shipment_id": ["SH-1001", "SH-1002", "SH-1003"]}, "sql": macro_query}) %}

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
