-- Test: Finance macro - FV, PV, PMT and NPER (Databricks SQL)
-- Validates the four closed-form time-value-of-money functions against Excel.
--
-- Sign convention: money leaving your pocket is negative, money arriving is positive.
--   rate=0.05, nper=10, pmt=-100 (deposit each period), pv=-1000 (initial deposit)
-- Runs on a real Databricks cluster; the default__ adapter emits unix_date() arithmetic and SELECT * EXCEPT.
--
-- Each case runs the macro and compares the appended result column
-- against the value Excel produces for the same inputs.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY VIEW finance_time_value_of_money_src AS
SELECT 0.05 AS rate_c, 10.0 AS nper_c, -100.0 AS pmt_c, -1000.0 AS pv_c, 0.0 AS fv_c, 10000.0 AS loan_pv_c, -1295.05 AS loan_pmt_c
{% endset %}
{% do run_query(create_src) %}

-- FV: future value of the deposits after 10 periods
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_time_value_of_money_src'],
    function_type='FV',
    output_column='finance_result',
    rate='rate_c',
    nper='nper_c',
    pmt='pmt_c',
    pv='pv_c',
    fv='',
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
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 2886.6839) | abs > 0.001 %}
    {{ exceptions.raise_compiler_error("Finance FV test FAILED: expected 2886.6839 (+/- 0.001), got " ~ actual) }}
{% endif %}

-- PV: present value of 10 payments of 100
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_time_value_of_money_src'],
    function_type='PV',
    output_column='finance_result',
    rate='rate_c',
    nper='nper_c',
    pmt='pmt_c',
    pv='',
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
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 772.1735) | abs > 0.001 %}
    {{ exceptions.raise_compiler_error("Finance PV test FAILED: expected 772.1735 (+/- 0.001), got " ~ actual) }}
{% endif %}

-- PMT: payment that clears a 10000 loan over 10 periods
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_time_value_of_money_src'],
    function_type='PMT',
    output_column='finance_result',
    rate='rate_c',
    nper='nper_c',
    pmt='',
    pv='loan_pv_c',
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
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - -1295.0457) | abs > 0.001 %}
    {{ exceptions.raise_compiler_error("Finance PMT test FAILED: expected -1295.0457 (+/- 0.001), got " ~ actual) }}
{% endif %}

-- NPER: number of periods that payment implies, back to 10
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_time_value_of_money_src'],
    function_type='NPER',
    output_column='finance_result',
    rate='rate_c',
    nper='',
    pmt='loan_pmt_c',
    pv='loan_pv_c',
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
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 10.0) | abs > 0.001 %}
    {{ exceptions.raise_compiler_error("Finance NPER test FAILED: expected 10.0 (+/- 0.001), got " ~ actual) }}
{% endif %}

{% endif %}

SELECT 1 WHERE 1=0
