-- Test: Finance macro - CAGR, EffectiveRate, NominalRate and FVSchedule (Snowflake)
-- Validates the growth-rate and rate-conversion functions.
--
-- EffectiveRate and NominalRate are inverses, so converting 12% nominal to
-- effective and back must return the original 12%.
-- Runs on Snowflake; the snowflake__ adapter emits datediff('day', ...) and SELECT * EXCLUDE.
--
-- Each case runs the macro and compares the appended result column
-- against the value Excel produces for the same inputs.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE FINANCE_RATE_CONVERSIONS_SRC AS
SELECT 10000.0 AS BEGIN_C, 19000.0 AS END_C, 5.0 AS PERIODS_C, 0.12 AS NOMINAL_C, 0.126825 AS EFFECT_C, 12.0 AS NPERY_C, 1000.0 AS PRINCIPAL_C, 0.05 AS R1, 0.06 AS R2, 0.07 AS R3
{% endset %}
{% do run_query(create_src) %}

-- CAGR: 10000 growing to 19000 over 5 periods
{% set macro_query %}
{{ prophecy_basics.Finance(
    relation_name=['FINANCE_RATE_CONVERSIONS_SRC'],
    function_type='CAGR',
    output_column='FINANCE_RESULT',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='',
    value_list='',
    date_list='',
    begin_value='BEGIN_C',
    end_value='END_C',
    periods='PERIODS_C',
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
    relation_name=['FINANCE_RATE_CONVERSIONS_SRC'],
    function_type='EffectiveRate',
    output_column='FINANCE_RESULT',
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
    nominal_rate='NOMINAL_C',
    effect_rate='',
    npery='NPERY_C',
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
    relation_name=['FINANCE_RATE_CONVERSIONS_SRC'],
    function_type='NominalRate',
    output_column='FINANCE_RESULT',
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
    effect_rate='EFFECT_C',
    npery='NPERY_C',
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
    relation_name=['FINANCE_RATE_CONVERSIONS_SRC'],
    function_type='FVSchedule',
    output_column='FINANCE_RESULT',
    rate='',
    nper='',
    pmt='',
    pv='',
    fv='',
    pay_type='0',
    principal='PRINCIPAL_C',
    value_list='R1,R2,R3',
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
