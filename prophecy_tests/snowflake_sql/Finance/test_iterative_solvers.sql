-- Test: Finance macro - IRR, XIRR and Rate (Snowflake)
-- Validates the three bisection solvers.
--
-- These are the only functions that build a chain of CTEs and drop working
-- columns from SELECT *, so they also cover the exclusion keyword this adapter
-- emits. 40 rounds narrow the starting bracket to roughly 1e-11, far inside the
-- tolerances below.
-- Runs on Snowflake; the snowflake__ adapter emits datediff('day', ...) and SELECT * EXCLUDE.
--
-- Each case runs the macro and compares the appended result column
-- against the value Excel produces for the same inputs.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE FINANCE_ITERATIVE_SOLVERS_SRC AS
SELECT -1000.0 AS OUT_C, 500.0 AS F1, 500.0 AS F2, 500.0 AS F3, 1100.0 AS IN_C, 10.0 AS NPER_C, -1295.05 AS PMT_C, 10000.0 AS PV_C, 0.0 AS FV_C, CAST('2024-01-01' AS DATE) AS D0, CAST('2024-12-31' AS DATE) AS D1
{% endset %}
{% do run_query(create_src) %}

-- IRR: -1000 then three inflows of 500
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['FINANCE_ITERATIVE_SOLVERS_SRC'],
    function_type='IRR',
    output_column='FINANCE_RESULT',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='OUT_C,F1,F2,F3',
    date_list='',
    begin_value='',
    end_value='',
    periods='',
    nominal_rate='',
    effect_rate='',
    npery='',
    finance_rate='',
    reinvest_rate='',
    lo_bound='',
    hi_bound='',
    n_iter='40'
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 0.233752) | abs > 0.00001 %}
    {{ exceptions.raise_compiler_error("Finance IRR test FAILED: expected 0.233752 (+/- 0.00001), got " ~ actual) }}
{% endif %}

-- XIRR: -1000 today against 1100 in 365 days is exactly 10%
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['FINANCE_ITERATIVE_SOLVERS_SRC'],
    function_type='XIRR',
    output_column='FINANCE_RESULT',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='OUT_C,IN_C',
    date_list='D0,D1',
    begin_value='',
    end_value='',
    periods='',
    nominal_rate='',
    effect_rate='',
    npery='',
    finance_rate='',
    reinvest_rate='',
    lo_bound='',
    hi_bound='',
    n_iter='40'
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 0.1) | abs > 0.000001 %}
    {{ exceptions.raise_compiler_error("Finance XIRR test FAILED: expected 0.1 (+/- 0.000001), got " ~ actual) }}
{% endif %}

-- Rate: the rate hidden in a 10000 loan repaid at 1295.05 for 10 periods
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['FINANCE_ITERATIVE_SOLVERS_SRC'],
    function_type='Rate',
    output_column='FINANCE_RESULT',
    rate='',
    nper='NPER_C',
    pmt='PMT_C',
    pv='PV_C',
    fv='FV_C',
    pay_type='0',
    principal='',
    value_list='',
    date_list='',
    begin_value='',
    end_value='',
    periods='',
    nominal_rate='',
    effect_rate='',
    npery='',
    finance_rate='',
    reinvest_rate='',
    lo_bound='',
    hi_bound='',
    n_iter='40'
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 0.05) | abs > 0.0001 %}
    {{ exceptions.raise_compiler_error("Finance Rate test FAILED: expected 0.05 (+/- 0.0001), got " ~ actual) }}
{% endif %}

{% endif %}

SELECT 1 WHERE 1=0
