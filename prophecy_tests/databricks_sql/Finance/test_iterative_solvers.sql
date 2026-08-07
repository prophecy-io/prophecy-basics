-- Test: Finance macro - IRR, XIRR and Rate (Databricks SQL)
-- Validates the three bisection solvers.
--
-- These are the only functions that build a chain of CTEs and drop working
-- columns from SELECT *, so they also cover the exclusion keyword this adapter
-- emits. 40 rounds narrow the starting bracket to roughly 1e-11, far inside the
-- tolerances below.
-- Runs on a real Databricks cluster; the default__ adapter emits unix_date() arithmetic and SELECT * EXCEPT.
--
-- Each case runs the macro and compares the appended result column
-- against the value Excel produces for the same inputs.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY VIEW finance_iterative_solvers_src AS
SELECT -1000.0 AS out_c, 500.0 AS f1, 500.0 AS f2, 500.0 AS f3, 1100.0 AS in_c, 10.0 AS nper_c, -1295.05 AS pmt_c, 10000.0 AS pv_c, 0.0 AS fv_c, CAST('2024-01-01' AS DATE) AS d0, CAST('2024-12-31' AS DATE) AS d1
{% endset %}
{% do run_query(create_src) %}

-- IRR: -1000 then three inflows of 500
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_iterative_solvers_src'],
    function_type='IRR',
    output_column='finance_result',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='out_c,f1,f2,f3',
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
    relation_name=['finance_iterative_solvers_src'],
    function_type='XIRR',
    output_column='finance_result',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='out_c,in_c',
    date_list='d0,d1',
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
    relation_name=['finance_iterative_solvers_src'],
    function_type='Rate',
    output_column='finance_result',
    rate='',
    nper='nper_c',
    pmt='pmt_c',
    pv='pv_c',
    fv='fv_c',
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
