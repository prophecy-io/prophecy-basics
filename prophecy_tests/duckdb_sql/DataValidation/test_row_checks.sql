-- Test: DataValidation macro - row-level checks on an orders table (DuckDB)
-- Each case embeds the macro in a CTE, the way Prophecy-generated models call it, then
-- compares the output column list and the ordered values (as text) with the expected result.
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
    SELECT 1 AS order_id, 'ann@shop.com' AS customer_email, 'shipped' AS status, 120 AS amount, DATE '2024-01-05' AS order_date, DATE '2024-01-07' AS ship_date
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
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "order_id", "dataType": "integer"}, {"name": "customer_email", "dataType": "string"}, {"name": "status", "dataType": "string"}, {"name": "amount", "dataType": "integer"}, {"name": "order_date", "dataType": "date"}, {"name": "ship_date", "dataType": "date"}]', [{'checkType': 'not_null', 'columns': ['order_id', 'customer_email']}, {'checkType': 'unique', 'columns': ['order_id']}, {'checkType': 'accepted_values', 'columns': ['status'], 'values': 'open, shipped, cancelled'}, {'checkType': 'value_range', 'columns': ['amount'], 'minValue': '0', 'maxValue': '10000'}, {'checkType': 'regex_match', 'columns': ['customer_email'], 'pattern': '[a-z]+@[a-z]+[.][a-z]+'}, {'checkType': 'expression', 'expression': 'ship_date is null or ship_date >= order_date', 'checkName': 'Ships after order'}], 'passed', 'continue') }}
)
SELECT * FROM validated ORDER BY order_id, amount
{% endset %}
{% do cases.append({"name": "passed rows keep only the input columns", "columns": ["order_id", "customer_email", "status", "amount", "order_date", "ship_date"], "expect": {"order_id": ["1", "2"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ orders_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "order_id", "dataType": "integer"}, {"name": "customer_email", "dataType": "string"}, {"name": "status", "dataType": "string"}, {"name": "amount", "dataType": "integer"}, {"name": "order_date", "dataType": "date"}, {"name": "ship_date", "dataType": "date"}]', [{'checkType': 'not_null', 'columns': ['order_id', 'customer_email']}, {'checkType': 'unique', 'columns': ['order_id']}, {'checkType': 'accepted_values', 'columns': ['status'], 'values': 'open, shipped, cancelled'}, {'checkType': 'value_range', 'columns': ['amount'], 'minValue': '0', 'maxValue': '10000'}, {'checkType': 'regex_match', 'columns': ['customer_email'], 'pattern': '[a-z]+@[a-z]+[.][a-z]+'}, {'checkType': 'expression', 'expression': 'ship_date is null or ship_date >= order_date', 'checkName': 'Ships after order'}], 'failed', 'continue') }}
)
SELECT * FROM validated ORDER BY order_id, amount
{% endset %}
{% do cases.append({"name": "failed rows name every check they failed", "columns": ["order_id", "customer_email", "status", "amount", "order_date", "ship_date", "dq_failed_checks"], "expect": {"order_id": ["3", "4", "5", "5", "6"], "dq_failed_checks": ["Not null: order_id, customer_email", "Accepted values: status; Pattern match: customer_email", "Unique: order_id; Value range: amount", "Unique: order_id", "Ships after order"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ orders_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "order_id", "dataType": "integer"}, {"name": "customer_email", "dataType": "string"}, {"name": "status", "dataType": "string"}, {"name": "amount", "dataType": "integer"}, {"name": "order_date", "dataType": "date"}, {"name": "ship_date", "dataType": "date"}]', [{'checkType': 'not_null', 'columns': ['order_id', 'customer_email']}, {'checkType': 'unique', 'columns': ['order_id']}, {'checkType': 'accepted_values', 'columns': ['status'], 'values': 'open, shipped, cancelled'}, {'checkType': 'value_range', 'columns': ['amount'], 'minValue': '0', 'maxValue': '10000'}, {'checkType': 'regex_match', 'columns': ['customer_email'], 'pattern': '[a-z]+@[a-z]+[.][a-z]+'}, {'checkType': 'expression', 'expression': 'ship_date is null or ship_date >= order_date', 'checkName': 'Ships after order'}], 'all', 'continue') }}
)
SELECT * FROM validated ORDER BY order_id, amount
{% endset %}
{% do cases.append({"name": "all rows carry dq_passed and dq_failed_checks", "columns": ["order_id", "customer_email", "status", "amount", "order_date", "ship_date", "dq_passed", "dq_failed_checks"], "expect": {"order_id": ["1", "2", "3", "4", "5", "5", "6"], "dq_passed": ["True", "True", "False", "False", "False", "False", "False"], "dq_failed_checks": ["None", "None", "Not null: order_id, customer_email", "Accepted values: status; Pattern match: customer_email", "Unique: order_id; Value range: amount", "Unique: order_id", "Ships after order"]}, "sql": macro_query}) %}

{% set macro_query %}
{{ orders_cte }},
validated AS (
    {{ prophecy_basics.DataValidation(['orders'], '[{"name": "order_id", "dataType": "integer"}, {"name": "customer_email", "dataType": "string"}, {"name": "status", "dataType": "string"}, {"name": "amount", "dataType": "integer"}, {"name": "order_date", "dataType": "date"}, {"name": "ship_date", "dataType": "date"}]', [{'checkType': 'disallowed_values', 'columns': ['status'], 'values': 'returned, lost'}, {'checkType': 'value_range', 'columns': ['order_date'], 'minValue': '2024-01-06', 'maxValue': '2024-01-08', 'checkName': 'Ordered in the promo week'}], 'failed', 'continue') }}
)
SELECT * FROM validated ORDER BY order_id, amount
{% endset %}
{% do cases.append({"name": "disallowed values and a date range", "columns": ["order_id", "customer_email", "status", "amount", "order_date", "ship_date", "dq_failed_checks"], "expect": {"order_id": ["1", "4", "6"], "dq_failed_checks": ["Ordered in the promo week", "Disallowed values: status", "Ordered in the promo week"]}, "sql": macro_query}) %}

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
