-- Test: DataValidation macro - row-level checks on an orders table (Snowflake)
-- Each case embeds the macro in a CTE, the way Prophecy-generated models call it, then
-- compares the output column list and the ordered values (as text) with the expected result.
-- The schema and columns use upper-case names, as Prophecy reports them for unquoted
-- Snowflake columns; check names and result columns follow that case.
--
-- The same six checks run against the same orders in every case; only the output changes:
--   order 1, 2  pass every check (order 2 is not shipped yet, so its ship_date is NULL)
--   order 3     has no customer_email                        -> Not null
--   order 4     status 'returned', email without a domain    -> Accepted values, Pattern match
--   order 5     appears twice, one copy with a negative amount -> Unique (both), Value range
--   order 6     shipped before it was ordered                -> Ships after order (custom SQL)

{% if execute %}
{% set cases = [] %}

{% set orders_cte %}
WITH orders AS (
    SELECT 1 AS ORDER_ID, 'ann@shop.com' AS CUSTOMER_EMAIL, 'shipped' AS STATUS, 120 AS AMOUNT, DATE '2024-01-05' AS ORDER_DATE, DATE '2024-01-07' AS SHIP_DATE
    UNION ALL SELECT 2, 'bob@shop.com', 'open', 35, DATE '2024-01-06', NULL
    UNION ALL SELECT 3, NULL, 'shipped', 80, DATE '2024-01-06', DATE '2024-01-08'
    UNION ALL SELECT 4, 'dan@shop', 'returned', 50, DATE '2024-01-07', DATE '2024-01-09'
    UNION ALL SELECT 5, 'eve@shop.com', 'open', -10, DATE '2024-01-08', NULL
    UNION ALL SELECT 5, 'eve@shop.com', 'open', 20, DATE '2024-01-08', NULL
    UNION ALL SELECT 6, 'fay@shop.com', 'shipped', 60, DATE '2024-01-10', DATE '2024-01-09'
)
{% endset %}

{% set macro_query %}
{{ orders_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "ORDER_ID", "dataType": "integer"}, {"name": "CUSTOMER_EMAIL", "dataType": "string"}, {"name": "STATUS", "dataType": "string"}, {"name": "AMOUNT", "dataType": "integer"}, {"name": "ORDER_DATE", "dataType": "date"}, {"name": "SHIP_DATE", "dataType": "date"}]', [{'checkType': 'not_null', 'columns': ['ORDER_ID', 'CUSTOMER_EMAIL']}, {'checkType': 'unique', 'columns': ['ORDER_ID']}, {'checkType': 'accepted_values', 'columns': ['STATUS'], 'values': 'open, shipped, cancelled'}, {'checkType': 'value_range', 'columns': ['AMOUNT'], 'minValue': '0', 'maxValue': '10000'}, {'checkType': 'regex_match', 'columns': ['CUSTOMER_EMAIL'], 'pattern': '[a-z]+@[a-z]+[.][a-z]+'}, {'checkType': 'expression', 'expression': 'SHIP_DATE is null or SHIP_DATE >= ORDER_DATE', 'checkName': 'Ships after order'}], 'passed', 'continue') }}
)
SELECT * FROM validated ORDER BY ORDER_ID, AMOUNT
{% endset %}
{% do cases.append({"name": "passed rows keep only the input columns", "columns": ["ORDER_ID", "CUSTOMER_EMAIL", "STATUS", "AMOUNT", "ORDER_DATE", "SHIP_DATE"], "expect": {"ORDER_ID": ["1", "2"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ orders_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "ORDER_ID", "dataType": "integer"}, {"name": "CUSTOMER_EMAIL", "dataType": "string"}, {"name": "STATUS", "dataType": "string"}, {"name": "AMOUNT", "dataType": "integer"}, {"name": "ORDER_DATE", "dataType": "date"}, {"name": "SHIP_DATE", "dataType": "date"}]', [{'checkType': 'not_null', 'columns': ['ORDER_ID', 'CUSTOMER_EMAIL']}, {'checkType': 'unique', 'columns': ['ORDER_ID']}, {'checkType': 'accepted_values', 'columns': ['STATUS'], 'values': 'open, shipped, cancelled'}, {'checkType': 'value_range', 'columns': ['AMOUNT'], 'minValue': '0', 'maxValue': '10000'}, {'checkType': 'regex_match', 'columns': ['CUSTOMER_EMAIL'], 'pattern': '[a-z]+@[a-z]+[.][a-z]+'}, {'checkType': 'expression', 'expression': 'SHIP_DATE is null or SHIP_DATE >= ORDER_DATE', 'checkName': 'Ships after order'}], 'failed', 'continue') }}
)
SELECT * FROM validated ORDER BY ORDER_ID, AMOUNT
{% endset %}
{% do cases.append({"name": "failed rows name every check they failed", "columns": ["ORDER_ID", "CUSTOMER_EMAIL", "STATUS", "AMOUNT", "ORDER_DATE", "SHIP_DATE", "DQ_FAILED_CHECKS"], "expect": {"ORDER_ID": ["3", "4", "5", "5", "6"], "DQ_FAILED_CHECKS": ["Not null: ORDER_ID, CUSTOMER_EMAIL", "Accepted values: STATUS; Pattern match: CUSTOMER_EMAIL", "Unique: ORDER_ID; Value range: AMOUNT", "Unique: ORDER_ID", "Ships after order"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ orders_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "ORDER_ID", "dataType": "integer"}, {"name": "CUSTOMER_EMAIL", "dataType": "string"}, {"name": "STATUS", "dataType": "string"}, {"name": "AMOUNT", "dataType": "integer"}, {"name": "ORDER_DATE", "dataType": "date"}, {"name": "SHIP_DATE", "dataType": "date"}]', [{'checkType': 'not_null', 'columns': ['ORDER_ID', 'CUSTOMER_EMAIL']}, {'checkType': 'unique', 'columns': ['ORDER_ID']}, {'checkType': 'accepted_values', 'columns': ['STATUS'], 'values': 'open, shipped, cancelled'}, {'checkType': 'value_range', 'columns': ['AMOUNT'], 'minValue': '0', 'maxValue': '10000'}, {'checkType': 'regex_match', 'columns': ['CUSTOMER_EMAIL'], 'pattern': '[a-z]+@[a-z]+[.][a-z]+'}, {'checkType': 'expression', 'expression': 'SHIP_DATE is null or SHIP_DATE >= ORDER_DATE', 'checkName': 'Ships after order'}], 'all', 'continue') }}
)
SELECT * FROM validated ORDER BY ORDER_ID, AMOUNT
{% endset %}
{% do cases.append({"name": "all rows carry DQ_PASSED and DQ_FAILED_CHECKS", "columns": ["ORDER_ID", "CUSTOMER_EMAIL", "STATUS", "AMOUNT", "ORDER_DATE", "SHIP_DATE", "DQ_PASSED", "DQ_FAILED_CHECKS"], "expect": {"ORDER_ID": ["1", "2", "3", "4", "5", "5", "6"], "DQ_PASSED": ["True", "True", "False", "False", "False", "False", "False"], "DQ_FAILED_CHECKS": ["None", "None", "Not null: ORDER_ID, CUSTOMER_EMAIL", "Accepted values: STATUS; Pattern match: CUSTOMER_EMAIL", "Unique: ORDER_ID; Value range: AMOUNT", "Unique: ORDER_ID", "Ships after order"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ orders_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "ORDER_ID", "dataType": "integer"}, {"name": "CUSTOMER_EMAIL", "dataType": "string"}, {"name": "STATUS", "dataType": "string"}, {"name": "AMOUNT", "dataType": "integer"}, {"name": "ORDER_DATE", "dataType": "date"}, {"name": "SHIP_DATE", "dataType": "date"}]', [{'checkType': 'disallowed_values', 'columns': ['STATUS'], 'values': 'returned, lost'}, {'checkType': 'value_range', 'columns': ['ORDER_DATE'], 'minValue': '2024-01-06', 'maxValue': '2024-01-08', 'checkName': 'Ordered in the promo week'}], 'failed', 'continue') }}
)
SELECT * FROM validated ORDER BY ORDER_ID, AMOUNT
{% endset %}
{% do cases.append({"name": "disallowed values and a date range", "columns": ["ORDER_ID", "CUSTOMER_EMAIL", "STATUS", "AMOUNT", "ORDER_DATE", "SHIP_DATE", "DQ_FAILED_CHECKS"], "expect": {"ORDER_ID": ["1", "4", "6"], "DQ_FAILED_CHECKS": ["Ordered in the promo week", "Disallowed values: STATUS", "Ordered in the promo week"]}, "sql": macro_query}) %}

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
