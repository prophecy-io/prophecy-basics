-- Test: GenerateRows macro - sequences without a source table (DuckDB)
-- Each case embeds the macro in a CTE, the way Prophecy-generated models call it, then
-- compares the output column list and the ordered values (as text) with the expected result.
--
--   1..10:         plain counter
--   max_rows:      a condition that never fails is cut off after max_rows rows
--   business days: generation stops at the first failing value (Saturday 2024-01-06) and
--                  does not resume on the following Monday

{% if execute %}
{% set cases = [] %}

{% set macro_query %}
WITH generated AS (
    {{ prophecy_basics.GenerateRows([], '', '1', 'n <= 10', 'n + 1', 'n', '100', 'recursive') }}
)
SELECT * FROM generated ORDER BY n
{% endset %}
{% do cases.append({"name": "counter from 1 to 10", "columns": ["n"], "expect": {"n": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]}, "sql": macro_query}) %}

{% set macro_query %}
WITH generated AS (
    {{ prophecy_basics.GenerateRows([], '', '1', 'n >= 1', 'n + 1', 'n', '5', 'recursive') }}
)
SELECT * FROM generated ORDER BY n
{% endset %}
{% do cases.append({"name": "max_rows caps an open-ended sequence", "columns": ["n"], "expect": {"n": ["1", "2", "3", "4", "5"]}, "sql": macro_query}) %}

{% set macro_query %}
WITH generated AS (
    {{ prophecy_basics.GenerateRows([], '', '2024-01-01', 'isodow(business_day) <= 5', 'business_day + 1', 'business_day', '100', 'recursive') }}
)
SELECT * FROM generated ORDER BY business_day
{% endset %}
{% do cases.append({"name": "business days stop at the first weekend day", "columns": ["business_day"], "expect": {"business_day": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]}, "sql": macro_query}) %}

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
