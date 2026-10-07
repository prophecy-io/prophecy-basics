-- Test: Finance macro - CAGR, EffectiveRate, NominalRate and FVSchedule (DuckDB)
-- Validates the growth-rate and rate-conversion functions.
--
-- EffectiveRate and NominalRate are inverses, so converting 12% nominal to
-- effective and back must return the original 12%.
-- Runs on DuckDB; the duckdb__ adapter emits date_diff('day', ...) and SELECT * EXCLUDE.
--
-- Each case runs the macro and compares the appended result column
-- against the value Excel produces for the same inputs.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE finance_rate_conversions_src AS
SELECT 10000.0 AS begin_c, 19000.0 AS end_c, 5.0 AS periods_c, 0.12 AS nominal_c, 0.126825 AS effect_c, 12.0 AS npery_c, 1000.0 AS principal_c, 0.05 AS r1, 0.06 AS r2, 0.07 AS r3
{% endset %}
{% do run_query(create_src) %}

-- CAGR: 10000 growing to 19000 over 5 periods
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_rate_conversions_src'],
    function_type='CAGR',
    output_column='finance_result',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='',
    date_list='',
    begin_value='begin_c',
    end_value='end_c',
    periods='periods_c',
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
{% if (actual - 0.136974) | abs > 0.00001 %}
    {{ exceptions.raise_compiler_error("Finance CAGR test FAILED: expected 0.136974 (+/- 0.00001), got " ~ actual) }}
{% endif %}

-- EffectiveRate: 12% nominal compounded monthly
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_rate_conversions_src'],
    function_type='EffectiveRate',
    output_column='finance_result',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='',
    date_list='',
    begin_value='',
    end_value='',
    periods='',
    nominal_rate='nominal_c',
    effect_rate='',
    npery='npery_c',
    finance_rate='',
    reinvest_rate='',
    lo_bound='',
    hi_bound='',
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 0.126825) | abs > 0.000001 %}
    {{ exceptions.raise_compiler_error("Finance EffectiveRate test FAILED: expected 0.126825 (+/- 0.000001), got " ~ actual) }}
{% endif %}

-- NominalRate: 12.6825% effective back to 12% nominal
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_rate_conversions_src'],
    function_type='NominalRate',
    output_column='finance_result',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='',
    date_list='',
    begin_value='',
    end_value='',
    periods='',
    nominal_rate='',
    effect_rate='effect_c',
    npery='npery_c',
    finance_rate='',
    reinvest_rate='',
    lo_bound='',
    hi_bound='',
    n_iter=''
) }}
{% endset %}
{% set results = run_query(macro_query) %}
{% set actual = results.columns[-1].values()[0] | float %}
{% if (actual - 0.12) | abs > 0.00001 %}
    {{ exceptions.raise_compiler_error("Finance NominalRate test FAILED: expected 0.12 (+/- 0.00001), got " ~ actual) }}
{% endif %}

-- FVSchedule: 1000 compounded at 5%, then 6%, then 7%
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['finance_rate_conversions_src'],
    function_type='FVSchedule',
    output_column='finance_result',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='principal_c',
    value_list='r1,r2,r3',
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
{% if (actual - 1190.91) | abs > 0.001 %}
    {{ exceptions.raise_compiler_error("Finance FVSchedule test FAILED: expected 1190.91 (+/- 0.001), got " ~ actual) }}
{% endif %}

{% endif %}

SELECT 1 WHERE 1=0
