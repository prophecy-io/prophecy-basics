{#
  Finance Macro Gem
  =================

  Computes a wide range of financial functions over the columns of an input
  table and appends the result as a new output column. Closed-form functions
  are evaluated in a single SELECT, while iterative functions (IRR / RATE /
  XIRR) are solved with a bisection method expressed as chained CTEs.

  Parameters:
    - relation_name (list): Relation identifier(s) to read from (e.g. `['source_table']` or `[ref('my_model')]`).
    - function_type (string): Finance function to compute. One of CAGR, EffectiveRate,
        NominalRate, FV, PV, PMT, NPER, NPV, XNPV, FVSchedule, IRR, MIRR, MXIRR, Rate,
        XIRR, XNPV. Defaults to "FV".
    - output_column (string): Name of the appended result column. Defaults to "finance_result".
    - rate, nper, pmt, pv, fv, pay_type (string): Column/expression inputs for the
        time-value-of-money functions (FV / PV / PMT / NPER / Rate).
    - principal (string): Principal column used by FVSchedule.
    - value_list (string): Comma-separated cash-flow / rate-schedule columns used by
        NPV, XNPV, FVSchedule, IRR, MIRR, MXIRR and XIRR.
    - date_list (string): Comma-separated date columns aligned to value_list, used by
        XNPV, XIRR and MXIRR.
    - begin_value, end_value, periods (string): Inputs for CAGR.
    - nominal_rate, effect_rate, npery (string): Inputs for EffectiveRate / NominalRate.
    - finance_rate, reinvest_rate (string): Inputs for MIRR / MXIRR.
    - lo_bound, hi_bound, n_iter (string): Bisection bounds and iteration count for
        the iterative solvers (IRR / RATE / XIRR).
    - date_diff_style (string): Date-difference dialect for date-based functions.
        One of "databricks", "snowflake", "bigquery" or "ansi". Defaults to "databricks".
    - exclude_keyword (string): SELECT * exclusion keyword for the iterative solvers.
        "EXCLUDE" (Snowflake/DuckDB) or "EXCEPT" (Databricks/BigQuery). Defaults to "EXCEPT"
        when left blank, unless date_diff_style is snowflake/ansi, which default to "EXCLUDE".

  Adapter Support:
    - Default (Databricks / Spark / Snowflake / DuckDB / BigQuery). Dialect-specific
      behaviour is handled via the date_diff_style and exclude_keyword parameters.

  Depends on schema parameter:
    Yes

  Macro Call Example:
    -- Future Value (FV)
    {{ prophecy_basics.Finance(['source_table'], 'FV', 'finance_result', 'rate', 'nper', 'pmt', 'pv', '0', '0', '0', '', '', '', '', '', '', '', '', '', '', '', '', '', 'databricks', 'EXCEPT') }}
    -- Generated SQL: SELECT *, <fv expression> AS finance_result FROM source_table
#}
{%- macro _dd(a, b, style) -%}
{%- set style_clean = style | trim | lower -%}
{%- if style_clean == 'databricks' -%}(unix_date(cast({{ a }} as date)) - unix_date(cast({{ b }} as date)))
{%- elif style_clean == 'snowflake' -%}datediff('day', cast({{ b }} as date), cast({{ a }} as date))
{%- elif style_clean == 'bigquery' -%}date_diff(cast({{ a }} as date), cast({{ b }} as date), day)
{%- else -%}(cast({{ a }} as date) - cast({{ b }} as date))
{%- endif -%}
{%- endmacro -%}

{%- macro _npv0_at(r, cols) -%}
( {% for c in cols %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r }}), {{ loop.index0 }}){% endfor %} )
{%- endmacro -%}

{%- macro _xnpv_at(r, cols, dts, dd_style) -%}
( {% for c in cols %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r }}), ({{ prophecy_basics._dd(dts[loop.index0] | trim, dts[0] | trim, dd_style) }}) / 365.0){% endfor %} )
{%- endmacro -%}

{%- macro _rate_f(r, nper, pmt, pv, fv, pay_type) -%}
( case when ({{ r }}) = 0 then ({{ pv }}) + ({{ pmt }}) * ({{ nper }}) + ({{ fv }})
       else ({{ pv }}) * power(1 + ({{ r }}), ({{ nper }}))
            + ({{ pmt }}) * (1 + ({{ r }}) * ({{ pay_type }})) * (power(1 + ({{ r }}), ({{ nper }})) - 1) / ({{ r }})
            + ({{ fv }})
  end )
{%- endmacro -%}

{%- macro _fin_obj(fn, r, cols, dts, dd_style, nper, pmt, pv, fv, pay_type) -%}
{%- if fn == 'irr' -%}{{ prophecy_basics._npv0_at(r, cols) }}
{%- elif fn == 'xirr' -%}{{ prophecy_basics._xnpv_at(r, cols, dts, dd_style) }}
{%- elif fn == 'rate' -%}{{ prophecy_basics._rate_f(r, nper, pmt, pv, fv, pay_type) }}
{%- endif -%}
{%- endmacro -%}

{# ============================================================= #}
{# Main macro (dispatch)                                         #}
{# ============================================================= #}

{% macro Finance(
        relation_name,
        function_type,
        output_column,
        rate,
        nper,
        pmt,
        pv,
        fv,
        pay_type,
        principal,
        value_list,
        date_list,
        begin_value,
        end_value,
        periods,
        nominal_rate,
        effect_rate,
        npery,
        finance_rate,
        reinvest_rate,
        lo_bound,
        hi_bound,
        n_iter,
        date_diff_style,
        exclude_keyword
    ) -%}
    {{ return(adapter.dispatch('Finance', 'prophecy_basics')(
        relation_name, function_type, output_column, rate, nper, pmt, pv, fv, pay_type,
        principal, value_list, date_list, begin_value, end_value, periods, nominal_rate,
        effect_rate, npery, finance_rate, reinvest_rate, lo_bound, hi_bound, n_iter,
        date_diff_style, exclude_keyword
    )) }}
{% endmacro %}

{%- macro default__Finance(
        relation_name,
        function_type,
        output_column,
        rate,
        nper,
        pmt,
        pv,
        fv,
        pay_type,
        principal,
        value_list,
        date_list,
        begin_value,
        end_value,
        periods,
        nominal_rate,
        effect_rate,
        npery,
        finance_rate,
        reinvest_rate,
        lo_bound,
        hi_bound,
        n_iter,
        date_diff_style,
        exclude_keyword
) %}
{%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
{%- set src = relation_list | join(', ') -%}
{%- set fn = (function_type | default('FV') | trim | lower) -%}
{%- set out_col = (output_column | default('finance_result') | trim) -%}

{# Safe parameter fallbacks #}
{%- set r_val     = rate          if (rate          | trim != '') else '0' -%}
{%- set n_val     = nper          if (nper          | trim != '') else '0' -%}
{%- set p_val     = pmt           if (pmt           | trim != '') else '0' -%}
{%- set pv_val    = pv            if (pv            | trim != '') else '0' -%}
{%- set fv_val    = fv            if (fv            | trim != '') else '0' -%}
{%- set type_val  = pay_type      if (pay_type      | trim != '') else '0' -%}
{%- set pr_val    = principal     if (principal     | trim != '') else '0' -%}
{%- set bv_val    = begin_value   if (begin_value   | trim != '') else '0' -%}
{%- set ev_val    = end_value     if (end_value     | trim != '') else '0' -%}
{%- set per_val   = periods       if (periods       | trim != '') else '1' -%}
{%- set nom_val   = nominal_rate  if (nominal_rate  | trim != '') else '0' -%}
{%- set eff_val   = effect_rate   if (effect_rate   | trim != '') else '0' -%}
{%- set npery_val = npery         if (npery         | trim != '') else '1' -%}
{%- set fr_val    = finance_rate  if (finance_rate  | trim != '') else '0' -%}
{%- set rr_val    = reinvest_rate if (reinvest_rate | trim != '') else '0' -%}
{%- set dd_style  = date_diff_style if (date_diff_style | trim != '') else 'databricks' -%}

{# ---------- ITERATIVE: IRR / RATE / XIRR (bisection) ---------- #}
{%- if fn in ['irr', 'rate', 'xirr'] -%}
{%- set N   = (n_iter | trim | int) if (n_iter and n_iter | trim != '') else 60 -%}
{%- set xkw = (exclude_keyword | trim) if (exclude_keyword and exclude_keyword | trim != '') else ('EXCEPT' if dd_style in ['databricks', 'bigquery'] else 'EXCLUDE') -%}
{%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else [] -%}
{%- set dts  = date_list.split(',')  if (date_list  and date_list  | trim != '') else [] -%}
{%- set query -%}
with _fin_b0 as (
    select *,
        ({{ lo_bound if lo_bound | trim != '' else '-0.99' }}) as _fin_lo,
        ({{ hi_bound if hi_bound | trim != '' else '10' }}) as _fin_hi,
        ({{ prophecy_basics._fin_obj(fn, '_fin_lo', cols, dts, dd_style, n_val, p_val, pv_val, fv_val, type_val) }}) as _fin_flo
    from {{ src }}
)
{%- for i in range(N) %}
, _fin_p{{ i }} as (
    select *, ((_fin_lo + _fin_hi) / 2.0) as _fin_mid from _fin_b{{ i }}
)
, _fin_q{{ i }} as (
    select *, ({{ prophecy_basics._fin_obj(fn, '_fin_mid', cols, dts, dd_style, n_val, p_val, pv_val, fv_val, type_val) }}) as _fin_fmid
    from _fin_p{{ i }}
)
, _fin_b{{ i + 1 }} as (
    select * {{ xkw }} (_fin_lo, _fin_hi, _fin_flo, _fin_mid, _fin_fmid),
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_mid  else _fin_lo  end as _fin_lo,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_hi   else _fin_mid end as _fin_hi,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_fmid else _fin_flo end as _fin_flo
    from _fin_q{{ i }}
)
{%- endfor %}
select * {{ xkw }} (_fin_lo, _fin_hi, _fin_flo),
    ((_fin_lo + _fin_hi) / 2.0) as {{ out_col }}
from _fin_b{{ N }}
{%- endset -%}
{{ return(query) }}

{# ---------- CLOSED FORM ---------- #}
{%- else -%}
{%- set expr -%}
{%- if fn == 'cagr' -%}
    power(({{ ev_val }}) / nullif(({{ bv_val }}), 0), 1.0 / ({{ per_val }})) - 1
{%- elif fn == 'effectiverate' -%}
    power(1 + ({{ nom_val }}) / nullif(({{ npery_val }}), 0), ({{ npery_val }})) - 1
{%- elif fn == 'nominalrate' -%}
    ({{ npery_val }}) * (power(1 + ({{ eff_val }}), 1.0 / nullif(({{ npery_val }}), 0)) - 1)
{%- elif fn == 'fv' -%}
    case when ({{ r_val }}) = 0 then -(({{ pv_val }}) + ({{ p_val }}) * ({{ n_val }}))
         else -(({{ pv_val }}) * power(1 + ({{ r_val }}), ({{ n_val }}))
              + ({{ p_val }}) * (1 + ({{ r_val }}) * ({{ type_val }}))
                * (power(1 + ({{ r_val }}), ({{ n_val }})) - 1) / ({{ r_val }}))
    end
{%- elif fn == 'pv' -%}
    case when ({{ r_val }}) = 0 then -(({{ fv_val }}) + ({{ p_val }}) * ({{ n_val }}))
         else -(({{ fv_val }})
              + ({{ p_val }}) * (1 + ({{ r_val }}) * ({{ type_val }}))
                * (power(1 + ({{ r_val }}), ({{ n_val }})) - 1) / ({{ r_val }}))
              / power(1 + ({{ r_val }}), ({{ n_val }}))
    end
{%- elif fn == 'pmt' -%}
    case when ({{ r_val }}) = 0 then -(({{ pv_val }}) + ({{ fv_val }})) / ({{ n_val }})
         else -(({{ pv_val }}) * power(1 + ({{ r_val }}), ({{ n_val }})) + ({{ fv_val }}))
              / ((1 + ({{ r_val }}) * ({{ type_val }}))
                 * (power(1 + ({{ r_val }}), ({{ n_val }})) - 1) / ({{ r_val }}))
    end
{%- elif fn == 'nper' -%}
    case when ({{ r_val }}) = 0 then -(({{ pv_val }}) + ({{ fv_val }})) / nullif(({{ p_val }}), 0)
         else ln( (({{ p_val }}) * (1 + ({{ r_val }}) * ({{ type_val }})) - ({{ fv_val }}) * ({{ r_val }}))
                / nullif((({{ p_val }}) * (1 + ({{ r_val }}) * ({{ type_val }})) + ({{ pv_val }}) * ({{ r_val }})), 0) )
              / ln(1 + ({{ r_val }}))
    end
{%- elif fn == 'npv' -%}
    {%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else ['0'] -%}
    ( {% for c in cols %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), {{ loop.index }}){% endfor %} )
{%- elif fn == 'xnpv' -%}
    {%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else ['0'] -%}
    {%- set dts = date_list.split(',') if (date_list and date_list | trim != '') else ['current_date()'] -%}
    {%- set d0 = dts[0] | trim -%}
    ( {% for c in cols %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), ({{ prophecy_basics._dd(dts[loop.index0] | trim, d0, dd_style) }}) / 365.0){% endfor %} )
{%- elif fn == 'fvschedule' -%}
    ({{ pr_val }}) {%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else [] -%} {%- for c in cols %} * (1 + ({{ c | trim }})){% endfor %}
{%- elif fn == 'mirr' -%}
    {%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else ['0','0'] -%}
    {%- set Nn = cols | length - 1 -%}
    power(
      -( {% for c in cols %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), {{ Nn - loop.index0 }}) else 0 end{% endfor %} )
      / nullif(( {% for c in cols %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), {{ loop.index0 }}) else 0 end{% endfor %} ), 0)
    , 1.0 / {{ Nn }}) - 1
{%- elif fn == 'mxirr' -%}
    {%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else ['0','0'] -%}
    {%- set dts = date_list.split(',') if (date_list and date_list | trim != '') else ['current_date()','current_date()'] -%}
    {%- set d0 = dts[0] | trim -%}
    {%- set dlast = dts[cols | length - 1] | trim -%}
    power(
      -( {% for c in cols %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), ({{ prophecy_basics._dd(dlast, d0, dd_style) }} - {{ prophecy_basics._dd(dts[loop.index0] | trim, d0, dd_style) }}) / 365.0) else 0 end{% endfor %} )
      / nullif(( {% for c in cols %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), ({{ prophecy_basics._dd(dts[loop.index0] | trim, d0, dd_style) }}) / 365.0) else 0 end{% endfor %} ), 0)
    , 365.0 / nullif({{ prophecy_basics._dd(dlast, d0, dd_style) }}, 0)) - 1
{%- else -%}
    null
{%- endif -%}
{%- endset -%}
{%- set query -%}
select *, {{ expr }} as {{ out_col }}
from {{ src }}
{%- endset -%}
{{ return(query) }}
{%- endif -%}
{%- endmacro -%}