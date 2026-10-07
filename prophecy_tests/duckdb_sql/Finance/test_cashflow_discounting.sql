-- Test: Finance macro - NPV, XNPV, MIRR and MXIRR (DuckDB)
-- Validates the multi-column cash-flow functions.
--
-- XNPV and MXIRR discount by actual days between dates, so these two cases also
-- cover the date arithmetic this adapter emits.
-- Runs on DuckDB; the duckdb__ adapter emits date_diff('day', ...) and SELECT * EXCLUDE.
--
-- Each case runs the macro and compares the appended result column
-- against the value Excel produces for the same inputs.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE finance_cashflow_discounting_src AS
SELECT 0.10 AS rate_c, 100.0 AS cf1, 200.0 AS cf2, 300.0 AS cf3, -1000.0 AS out_c, 1100.0 AS in_c, 1200.0 AS in2_c, 400.0 AS f1, 500.0 AS f2, 600.0 AS f3, 0.10 AS fin_c, 0.12 AS rei_c, CAST('2024-01-01' AS DATE) AS d0, CAST('2024-12-31' AS DATE) AS d1, CAST('2025-01-01' AS DATE) AS d2
{% endset %}
{% do run_query(create_src) %}

-- NPV: 100, 200, 300 discounted at 10%
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_cashflow_discounting_src'],
    function_type='NPV',
    output_column='finance_result',
    rate='rate_c',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='cf1,cf2,cf3',
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
{% if (actual - 481.5928) | abs > 0.001 %}
    {{ exceptions.raise_compiler_error("Finance NPV test FAILED: expected 481.5928 (+/- 0.001), got " ~ actual) }}
{% endif %}

-- XNPV: -1000 today against 1100 in 365 days at 10% is a wash
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_cashflow_discounting_src'],
    function_type='XNPV',
    output_column='finance_result',
    rate='rate_c',
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
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 0.0) | abs > 0.000001 %}
    {{ exceptions.raise_compiler_error("Finance XNPV test FAILED: expected 0.0 (+/- 0.000001), got " ~ actual) }}
{% endif %}

-- MIRR: -1000 then 400, 500, 600 at 10% finance / 12% reinvest
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_cashflow_discounting_src'],
    function_type='MIRR',
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
    finance_rate='fin_c',
    reinvest_rate='rei_c',
    lo_bound='',
    hi_bound='',
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 0.184466) | abs > 0.00001 %}
    {{ exceptions.raise_compiler_error("Finance MIRR test FAILED: expected 0.184466 (+/- 0.00001), got " ~ actual) }}
{% endif %}

-- MXIRR: -1000 today against 1200 in 366 days
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_cashflow_discounting_src'],
    function_type='MXIRR',
    output_column='finance_result',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='out_c,in2_c',
    date_list='d0,d2',
    begin_value='',
    end_value='',
    periods='',
    nominal_rate='',
    effect_rate='',
    npery='',
    finance_rate='fin_c',
    reinvest_rate='rei_c',
    lo_bound='',
    hi_bound='',
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 0.199402) | abs > 0.00001 %}
    {{ exceptions.raise_compiler_error("Finance MXIRR test FAILED: expected 0.199402 (+/- 0.00001), got " ~ actual) }}
{% endif %}

{% endif %}

SELECT 1 WHERE 1=0
