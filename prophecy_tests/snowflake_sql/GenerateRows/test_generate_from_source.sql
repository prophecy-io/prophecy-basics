-- Test: GenerateRows macro - expanding every source row into a sequence (Snowflake)
-- Each case embeds the macro in a CTE, the way Prophecy-generated models call it, then
-- compares the output column list and the ordered values (as text) with the expected result.
-- The schema lists upper-case names, as Prophecy reports them for unquoted Snowflake columns;
-- the snowflake__ adapter needs it to build the recursive CTE's column list.
--
--   orders:        one row per ordered unit; an order with qty 0 produces no rows
--   subscriptions: weekly billing dates from start_date through end_date (inclusive)
--   loans:         repayment schedule; the generated balance replaces the source balance
--                  column, while payload.balance still reads the source value

{% if execute %}
{% set cases = [] %}

{% set macro_query %}
WITH orders AS (
    SELECT 101 AS order_id, 3 AS qty
    UNION ALL SELECT 102, 1
    UNION ALL SELECT 103, 0
),
generated AS (
    {{ prophecy_basics.GenerateRows(
        ['orders'],
        '[{"name": "ORDER_ID", "dataType": "integer"}, {"name": "QTY", "dataType": "integer"}]',
        '1',
        'unit_no <= payload.qty',
        'unit_no + 1',
        'unit_no',
        '100',
        'recursive'
    ) }}
)
SELECT * FROM generated ORDER BY order_id, unit_no
{% endset %}
{% do cases.append({"name": "one row per ordered unit", "columns": ["ORDER_ID", "QTY", "UNIT_NO"], "expect": {"ORDER_ID": ["101", "101", "101", "102"], "UNIT_NO": ["1", "2", "3", "1"]}, "sql": macro_query}) %}

{% set macro_query %}
WITH subscriptions AS (
    SELECT 1 AS sub_id, TO_DATE('2024-01-01') AS start_date, TO_DATE('2024-01-29') AS end_date
    UNION ALL SELECT 2, TO_DATE('2024-02-10'), TO_DATE('2024-02-20')
),
generated AS (
    {{ prophecy_basics.GenerateRows(
        ['subscriptions'],
        '[{"name": "SUB_ID", "dataType": "integer"}, {"name": "START_DATE", "dataType": "date"}, {"name": "END_DATE", "dataType": "date"}]',
        'payload.start_date',
        'billing_date <= payload.end_date',
        'DATEADD(day, 7, billing_date)',
        'billing_date',
        '100',
        'recursive'
    ) }}
)
SELECT * FROM generated ORDER BY sub_id, billing_date
{% endset %}
{% do cases.append({"name": "weekly billing dates through end_date", "columns": ["SUB_ID", "START_DATE", "END_DATE", "BILLING_DATE"], "expect": {"SUB_ID": ["1", "1", "1", "1", "1", "2", "2"], "BILLING_DATE": ["2024-01-01", "2024-01-08", "2024-01-15", "2024-01-22", "2024-01-29", "2024-02-10", "2024-02-17"]}, "sql": macro_query}) %}

{% set macro_query %}
WITH loans AS (
    SELECT 1 AS loan_id, 1000 AS balance, 300 AS monthly_payment
    UNION ALL SELECT 2, 500, 500
),
generated AS (
    {{ prophecy_basics.GenerateRows(
        ['loans'],
        '[{"name": "LOAN_ID", "dataType": "integer"}, {"name": "BALANCE", "dataType": "integer"}, {"name": "MONTHLY_PAYMENT", "dataType": "integer"}]',
        'payload.balance',
        'balance > 0',
        'balance - payload.monthly_payment',
        'balance',
        '100',
        'recursive'
    ) }}
)
SELECT * FROM generated ORDER BY loan_id, balance DESC
{% endset %}
{% do cases.append({"name": "repayment schedule replaces the source balance column", "columns": ["LOAN_ID", "MONTHLY_PAYMENT", "BALANCE"], "expect": {"LOAN_ID": ["1", "1", "1", "1", "2"], "BALANCE": ["1000", "700", "400", "100", "500"]}, "sql": macro_query}) %}

{% for case in cases %}
    {% set results = run_query(case.sql) %}
    {% if (results.column_names | list) != case.columns %}
        {{ exceptions.raise_compiler_error("GenerateRows test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
    {% endif %}
    {% for col, expected in case.expect.items() %}
        {% set actual = results.columns[col].values() | map('string') | list %}
        {% if actual != expected %}
            {{ exceptions.raise_compiler_error("GenerateRows test '" ~ case.name ~ "' FAILED on column " ~ col ~ ": expected " ~ expected ~ ", got " ~ actual) }}
        {% endif %}
    {% endfor %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
