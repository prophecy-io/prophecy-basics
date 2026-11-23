-- Test: DataQuality macro - what happens when checks fail (BigQuery)
-- Each case embeds the macro in a CTE, the way Prophecy-generated models call it, then
-- compares the output column list and the ordered values (as text) with the expected result.
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
    SELECT 'SH-1001' AS shipment_id, 'DE' AS country, 12 AS weight_kg
    UNION ALL SELECT 'SH-1002', 'FR', 4
    UNION ALL SELECT 'SH-1003', 'XX', 7
)
{% endset %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataQuality(['shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'required_columns', 'columns': ['shipment_id', 'weight_kg']}, {'checkType': 'row_count', 'minValue': '5'}, {'checkType': 'regex_match', 'columns': ['shipment_id'], 'pattern': 'SH-[0-9]{4}'}, {'checkType': 'value_range', 'columns': ['weight_kg'], 'maxValue': '10', 'checkName': 'Parcel weight limit', 'reasonCode': 'OVERWEIGHT'}], 'summary', 'continue') }}
)
SELECT * FROM validated ORDER BY check_number
{% endset %}
{% do cases.append({"name": "summary: one row per check", "columns": ["check_number", "check_name", "check_type", "reason_code", "failed_rows", "check_passed"], "expect": {"check_name": ["Required columns: shipment_id, weight_kg", "Row count", "Pattern match: shipment_id", "Parcel weight limit"], "check_type": ["required_columns", "row_count", "regex_match", "value_range"], "reason_code": ["REQUIRED_COLUMNS", "ROW_COUNT", "REGEX_MATCH", "OVERWEIGHT"], "failed_rows": ["0", "3", "0", "1"], "check_passed": ["True", "False", "True", "False"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataQuality(['shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'required_columns', 'columns': ['shipment_id', 'weight_kg']}, {'checkType': 'row_count', 'minValue': '5'}, {'checkType': 'regex_match', 'columns': ['shipment_id'], 'pattern': 'SH-[0-9]{4}'}, {'checkType': 'value_range', 'columns': ['weight_kg'], 'maxValue': '10', 'checkName': 'Parcel weight limit', 'reasonCode': 'OVERWEIGHT'}], 'errors', 'continue') }}
)
SELECT * FROM validated ORDER BY check_number
{% endset %}
{% do cases.append({"name": "errors: one row per failed check, with a message", "columns": ["check_number", "check_name", "check_type", "reason_code", "failed_rows", "message"], "expect": {"check_number": ["2", "4"], "check_name": ["Row count", "Parcel weight limit"], "failed_rows": ["3", "1"], "message": ["The input has 3 rows, which is outside the allowed row count", "1 row(s) failed the check Parcel weight limit"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataQuality(['shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'value_range', 'columns': ['weight_kg'], 'maxValue': '10'}], 'passed', 'block') }}
)
SELECT * FROM validated ORDER BY shipment_id
{% endset %}
{% do cases.append({"name": "block: the passed output is empty when a check fails", "columns": ["shipment_id", "country", "weight_kg"], "expect": {"shipment_id": []}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataQuality(['shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'value_range', 'columns': ['weight_kg'], 'maxValue': '10'}], 'passed', 'block', '1', false) }}
)
SELECT * FROM validated ORDER BY shipment_id
{% endset %}
{% do cases.append({"name": "block: failing rows within the allowed number let passed rows through", "columns": ["shipment_id", "country", "weight_kg"], "expect": {"shipment_id": ["SH-1002", "SH-1003"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataQuality(['shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'not_null', 'columns': ['shipment_id', 'country']}, {'checkType': 'unique', 'columns': ['shipment_id']}, {'checkType': 'row_count', 'minValue': '1', 'maxValue': '1000'}], 'passed', 'fail') }}
)
SELECT * FROM validated ORDER BY shipment_id
{% endset %}
{% do cases.append({"name": "fail: clean data flows through unchanged", "columns": ["shipment_id", "country", "weight_kg"], "expect": {"shipment_id": ["SH-1001", "SH-1002", "SH-1003"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
validated AS (
    {{ prophecy_basics.DataQuality(['shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'value_range', 'columns': ['weight_kg'], 'maxValue': '10'}], 'all', 'fail', '1', false) }}
)
SELECT * FROM validated ORDER BY shipment_id
{% endset %}
{% do cases.append({"name": "fail: failing rows within the allowed number flow through", "columns": ["shipment_id", "country", "weight_kg", "dq_passed", "dq_failed_checks"], "expect": {"shipment_id": ["SH-1001", "SH-1002", "SH-1003"], "dq_passed": ["False", "True", "True"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ shipments_cte }},
no_shipments AS (
    SELECT * FROM shipments WHERE weight_kg < 0
),
validated AS (
    {{ prophecy_basics.DataQuality(['no_shipments'], '[{"name": "shipment_id", "dataType": "string"}, {"name": "country", "dataType": "string"}, {"name": "weight_kg", "dataType": "integer"}]', [{'checkType': 'not_null', 'columns': ['shipment_id']}, {'checkType': 'row_count', 'maxValue': '10'}], 'passed', 'fail', '', false) }}
)
SELECT * FROM validated ORDER BY shipment_id
{% endset %}
{% do cases.append({"name": "fail: an empty input with no failures returns no rows", "columns": ["shipment_id", "country", "weight_kg"], "expect": {"shipment_id": []}, "sql": macro_query}) %}

{% for case in cases %}
    {% set results = run_query(case.sql) %}
    {% if (results.column_names | list) != case.columns %}
        {{ exceptions.raise_compiler_error("DataQuality test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | map('string') | list %}
        {% if actual != expected %}
            {{ exceptions.raise_compiler_error("DataQuality test '" ~ case.name ~ "' FAILED on column " ~ col ~ ": expected " ~ expected ~ ", got " ~ actual) }}
        {% endif %}
    {% endfor %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
