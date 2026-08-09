-- Test: Finance macro - NPV, XNPV, MIRR and MXIRR (Snowflake)
-- Validates the multi-column cash-flow functions.
--
-- XNPV and MXIRR discount by actual days between dates, so these two cases also
-- cover the date arithmetic this adapter emits.
-- Runs on Snowflake; the snowflake__ adapter emits datediff('day', ...) and SELECT * EXCLUDE.
--
-- Each case runs the macro and compares the appended result column
-- against the value Excel produces for the same inputs.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE FINANCE_CASHFLOW_DISCOUNTING_SRC AS
SELECT 0.10 AS RATE_C, 100.0 AS CF1, 200.0 AS CF2, 300.0 AS CF3, -1000.0 AS OUT_C, 1100.0 AS IN_C, 1200.0 AS IN2_C, 400.0 AS F1, 500.0 AS F2, 600.0 AS F3, 0.10 AS FIN_C, 0.12 AS REI_C, CAST('2024-01-01' AS DATE) AS D0, CAST('2024-12-31' AS DATE) AS D1, CAST('2025-01-01' AS DATE) AS D2
{% endset %}
{% do run_query(create_src) %}

-- NPV: 100, 200, 300 discounted at 10%
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['FINANCE_CASHFLOW_DISCOUNTING_SRC'],
    function_type='NPV',
    output_column='FINANCE_RESULT',
    rate='RATE_C',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='CF1,CF2,CF3',
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
    relation_name=['FINANCE_CASHFLOW_DISCOUNTING_SRC'],
    function_type='XNPV',
    output_column='FINANCE_RESULT',
    rate='RATE_C',
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
    relation_name=['FINANCE_CASHFLOW_DISCOUNTING_SRC'],
    function_type='MIRR',
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
    finance_rate='FIN_C',
    reinvest_rate='REI_C',
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
    relation_name=['FINANCE_CASHFLOW_DISCOUNTING_SRC'],
    function_type='MXIRR',
    output_column='FINANCE_RESULT',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='OUT_C,IN2_C',
    date_list='D0,D2',
    begin_value='',
    end_value='',
    periods='',
    nominal_rate='',
    effect_rate='',
    npery='',
    finance_rate='FIN_C',
    reinvest_rate='REI_C',
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
