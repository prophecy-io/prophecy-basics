-- Test: DataValidation macro - what happens when checks fail (Snowflake)
-- Each case embeds the macro in a CTE, the way Prophecy-generated models call it, then
-- compares the output column list and the ordered values (as text) with the expected result.
-- The schema and columns use upper-case names, as Prophecy reports them for unquoted
-- Snowflake columns; check names and result columns follow that case.
--
--   summary: one row per check, including the table-level row count and required columns checks
--   errors:  one row per failed check, with a message
--   block:   the passed output is empty when checks fail, unless within the allowed number
--   fail:    clean data flows through unchanged, failing rows within the allowed number flow
--            through, and an empty input with no failures returns no rows. A failing run stops
--            the query, which a dbt test cannot catch, so that path is covered in the README instead.

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
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'required_columns', 'columns': ['SHIPMENT_ID', 'WEIGHT_KG']}, {'checkType': 'row_count', 'minValue': '5'}, {'checkType': 'regex_match', 'columns': ['SHIPMENT_ID'], 'pattern': 'SH-[0-9]{4}'}, {'checkType': 'value_range', 'columns': ['WEIGHT_KG'], 'maxValue': '10', 'checkName': 'Parcel weight limit', 'reasonCode': 'OVERWEIGHT'}], 'summary', 'continue') }}
)
SELECT * FROM validated ORDER BY CHECK_NUMBER
{% endset %}
{% do cases.append({"name": "summary: one row per check", "columns": ["CHECK_NUMBER", "CHECK_NAME", "CHECK_TYPE", "REASON_CODE", "FAILED_ROWS", "CHECK_PASSED"], "expect": {"CHECK_NAME": ["Required columns: SHIPMENT_ID, WEIGHT_KG", "Row count", "Pattern match: SHIPMENT_ID", "Parcel weight limit"], "CHECK_TYPE": ["required_columns", "row_count", "regex_match", "value_range"], "REASON_CODE": ["REQUIRED_COLUMNS", "ROW_COUNT", "REGEX_MATCH", "OVERWEIGHT"], "FAILED_ROWS": ["0", "3", "0", "1"], "CHECK_PASSED": ["True", "False", "True", "False"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'required_columns', 'columns': ['SHIPMENT_ID', 'WEIGHT_KG']}, {'checkType': 'row_count', 'minValue': '5'}, {'checkType': 'regex_match', 'columns': ['SHIPMENT_ID'], 'pattern': 'SH-[0-9]{4}'}, {'checkType': 'value_range', 'columns': ['WEIGHT_KG'], 'maxValue': '10', 'checkName': 'Parcel weight limit', 'reasonCode': 'OVERWEIGHT'}], 'errors', 'continue') }}
)
SELECT * FROM validated ORDER BY CHECK_NUMBER
{% endset %}
{% do cases.append({"name": "errors: one row per failed check, with a message", "columns": ["CHECK_NUMBER", "CHECK_NAME", "CHECK_TYPE", "REASON_CODE", "FAILED_ROWS", "MESSAGE"], "expect": {"CHECK_NUMBER": ["2", "4"], "CHECK_NAME": ["Row count", "Parcel weight limit"], "FAILED_ROWS": ["3", "1"], "MESSAGE": ["The input has 3 rows, which is outside the allowed row count", "1 row(s) failed the check Parcel weight limit"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'value_range', 'columns': ['WEIGHT_KG'], 'maxValue': '10'}], 'passed', 'block') }}
)
SELECT * FROM validated ORDER BY SHIPMENT_ID
{% endset %}
{% do cases.append({"name": "block: the passed output is empty when a check fails", "columns": ["SHIPMENT_ID", "COUNTRY", "WEIGHT_KG"], "expect": {"SHIPMENT_ID": []}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'value_range', 'columns': ['WEIGHT_KG'], 'maxValue': '10'}], 'passed', 'block', '1', false) }}
)
SELECT * FROM validated ORDER BY SHIPMENT_ID
{% endset %}
{% do cases.append({"name": "block: failing rows within the allowed number let passed rows through", "columns": ["SHIPMENT_ID", "COUNTRY", "WEIGHT_KG"], "expect": {"SHIPMENT_ID": ["SH-1002", "SH-1003"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'not_null', 'columns': ['SHIPMENT_ID', 'COUNTRY']}, {'checkType': 'unique', 'columns': ['SHIPMENT_ID']}, {'checkType': 'row_count', 'minValue': '1', 'maxValue': '1000'}], 'passed', 'fail') }}
)
SELECT * FROM validated ORDER BY SHIPMENT_ID
{% endset %}
{% do cases.append({"name": "fail: clean data flows through unchanged", "columns": ["SHIPMENT_ID", "COUNTRY", "WEIGHT_KG"], "expect": {"SHIPMENT_ID": ["SH-1001", "SH-1002", "SH-1003"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'value_range', 'columns': ['WEIGHT_KG'], 'maxValue': '10'}], 'all', 'fail', '1', false) }}
)
SELECT * FROM validated ORDER BY SHIPMENT_ID
{% endset %}
{% do cases.append({"name": "fail: failing rows within the allowed number flow through", "columns": ["SHIPMENT_ID", "COUNTRY", "WEIGHT_KG", "DQ_PASSED", "DQ_FAILED_CHECKS"], "expect": {"SHIPMENT_ID": ["SH-1001", "SH-1002", "SH-1003"], "DQ_PASSED": ["False", "True", "True"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
no_shipments AS (
    SELECT * FROM shipments WHERE WEIGHT_KG < 0
),
validated AS (
    {{ prophecy_basics.DataValidation(['no_shipments'], '[{"name": "SHIPMENT_ID", "dataType": "string"}, {"name": "COUNTRY", "dataType": "string"}, {"name": "WEIGHT_KG", "dataType": "integer"}]', [{'checkType': 'not_null', 'columns': ['SHIPMENT_ID']}, {'checkType': 'row_count', 'maxValue': '10'}], 'passed', 'fail', '', false) }}
)
SELECT * FROM validated ORDER BY SHIPMENT_ID
{% endset %}
{% do cases.append({"name": "fail: an empty input with no failures returns no rows", "columns": ["SHIPMENT_ID", "COUNTRY", "WEIGHT_KG"], "expect": {"SHIPMENT_ID": []}, "sql": macro_query}) %}

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
