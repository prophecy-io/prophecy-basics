{#
  Finance Macro Gem
  =================

  Computes a financial function over the columns of an input table and appends the
  result as a new column. Closed-form functions are evaluated in a single SELECT,
  while the iterative functions (IRR / RATE / XIRR) are solved with a bisection
  method expressed as a chain of CTEs.

  Parameters:
    - relation_name (list): Source relation(s).
    - function_type (string): CAGR, EffectiveRate, NominalRate, FV, PV, PMT, NPER, NPV,
        XNPV, FVSchedule, IRR, MIRR, MXIRR, Rate or XIRR. Defaults to "FV".
    - output_column (string): Name of the appended result column. Defaults to "finance_result".
    - rate, nper, pmt, pv, fv, pay_type: Time-value-of-money inputs used by
        FV / PV / PMT / NPER / Rate. `rate` is also the discount rate for NPV and XNPV.
    - principal: Starting amount for FVSchedule.
    - value_list (string): Comma-separated cash-flow (or rate-schedule) columns, used by
        NPV, XNPV, FVSchedule, IRR, MIRR, MXIRR and XIRR.
    - date_list (string): Comma-separated date columns aligned to value_list, used by
        XNPV, XIRR and MXIRR.
    - begin_value, end_value, periods: Inputs for CAGR.
    - nominal_rate, effect_rate, npery: Inputs for EffectiveRate / NominalRate.
    - finance_rate, reinvest_rate: Inputs for MIRR / MXIRR.
    - lo_bound, hi_bound, n_iter: Bisection bracket and iteration count for IRR / Rate /
        XIRR. Blank falls back to -0.99, 10 and 60.

  Adapter Support:
    - default__ (Databricks), snowflake__, bigquery__, duckdb__

    Each adapter is self-contained. They differ in exactly two places: how many days
    lie between two dates, and which keyword drops a column from SELECT *.

      Adapter      Day difference                              Drop column
      -----------  ------------------------------------------  -----------
      default__    unix_date(a) - unix_date(b)                  EXCEPT
      snowflake__  datediff('day', b, a)                        EXCLUDE
      bigquery__   date_diff(a, b, day)                         EXCEPT
      duckdb__     date_diff('day', b, a)                       EXCLUDE

    The warehouse decides which one runs, so no dialect has to be chosen in the gem.

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    -- Future value of a 10-period annuity
    {{ prophecy_basics.Finance(['src'], 'FV', 'fv_result', 'rate', 'nper', 'pmt', 'pv', '', '0', '', '', '', '', '', '', '', '', '', '', '', '', '', '') }}
    -- Net present value across three cash-flow columns
    {{ prophecy_basics.Finance(['src'], 'NPV', 'npv', 'r', '', '', '', '', '0', '', 'cf1,cf2,cf3', '', '', '', '', '', '', '', '', '', '', '', '') }}

  CTE Usage Example:
    Macro call:
      {{ prophecy_basics.Finance(['t'], 'NPV', 'npv', 'r', '', '', '', '', '0', '', 'cf1,cf2', '', '', '', '', '', '', '', '', '', '', '', '') }}

    Resolved query (default__):
      select *, ( (cf1) / power(1 + (r), 1) + (cf2) / power(1 + (r), 2) ) as npv
      from t
#}
{% macro Finance(relation_name,
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
    n_iter) -%}
    {{ return(adapter.dispatch('Finance', 'prophecy_basics')(relation_name,
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
    n_iter)) }}
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
    n_iter
) %}

{%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
{%- set src = relation_list | join(', ') -%}
{%- set fn = (function_type | default('FV') | trim | lower) -%}
{%- set out_col = (output_column | default('finance_result') | trim) -%}

{# A blank input falls back to a neutral value so the expression still compiles. #}
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

{%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else [] -%}
{%- set dts  = date_list.split(',')  if (date_list  and date_list  | trim != '') else [] -%}

{# Whole days from the first date column to each date column, Databricks style. #}
{%- set day_offsets = [] -%}
{%- for d in dts -%}
    {%- do day_offsets.append("(unix_date(cast(" ~ (d | trim) ~ " as date)) - unix_date(cast(" ~ (dts[0] | trim) ~ " as date)))") -%}
{%- endfor -%}

{%- if fn in ['irr', 'rate', 'xirr'] -%}

{%- set n_req = (n_iter | trim | int) if (n_iter and n_iter | trim != '') else 60 -%}
{%- set N = n_req if n_req > 0 else 60 -%}

{# The objective the solver drives to zero, rendered once per bracket variable. #}
{%- set obj = {} -%}
{%- for var in ['_fin_lo', '_fin_mid'] -%}
    {%- if fn == 'rate' -%}
        {%- do obj.update({var:
            '( case when (' ~ var ~ ') = 0 then (' ~ pv_val ~ ') + (' ~ p_val ~ ') * (' ~ n_val ~ ') + (' ~ fv_val ~ ')'
            ~ ' else (' ~ pv_val ~ ') * power(1 + (' ~ var ~ '), (' ~ n_val ~ '))'
            ~ ' + (' ~ p_val ~ ') * (1 + (' ~ var ~ ') * (' ~ type_val ~ '))'
            ~ ' * (power(1 + (' ~ var ~ '), (' ~ n_val ~ ')) - 1) / (' ~ var ~ ')'
            ~ ' + (' ~ fv_val ~ ') end )'}) -%}
    {%- else -%}
        {%- set parts = [] -%}
        {%- for c in cols -%}
            {%- if fn == 'irr' -%}
                {%- do parts.append('(' ~ (c | trim) ~ ') / power(1 + (' ~ var ~ '), ' ~ loop.index0 ~ ')') -%}
            {%- else -%}
                {%- set gap = day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' -%}
                {%- do parts.append('(' ~ (c | trim) ~ ') / power(1 + (' ~ var ~ '), (' ~ gap ~ ') / 365.0)') -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do obj.update({var: '( ' ~ (parts | join(' + ') if parts else '0') ~ ' )'}) -%}
    {%- endif -%}
{%- endfor -%}

{# The bracket and the objective at its lower end sit in separate CTEs on purpose:
   reading a SELECT-list alias from the same SELECT list is a lateral column alias,
   which not every warehouse accepts. #}
{%- set query -%}
with _fin_seed as (
    select *,
        ({{ lo_bound if (lo_bound and lo_bound | trim != '') else '-0.99' }}) as _fin_lo,
        ({{ hi_bound if (hi_bound and hi_bound | trim != '') else '10' }}) as _fin_hi
    from {{ src }}
)
, _fin_b0 as (
    select *, ({{ obj['_fin_lo'] }}) as _fin_flo
    from _fin_seed
)
{%- for i in range(N) %}
, _fin_p{{ i }} as (
    select *, ((_fin_lo + _fin_hi) / 2.0) as _fin_mid from _fin_b{{ i }}
)
, _fin_q{{ i }} as (
    select *, ({{ obj['_fin_mid'] }}) as _fin_fmid from _fin_p{{ i }}
)
, _fin_b{{ i + 1 }} as (
    select * EXCEPT (_fin_lo, _fin_hi, _fin_flo, _fin_mid, _fin_fmid),
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_mid  else _fin_lo  end as _fin_lo,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_hi   else _fin_mid end as _fin_hi,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_fmid else _fin_flo end as _fin_flo
    from _fin_q{{ i }}
)
{%- endfor %}
select * EXCEPT (_fin_lo, _fin_hi, _fin_flo),
    ((_fin_lo + _fin_hi) / 2.0) as {{ out_col }}
from _fin_b{{ N }}
{%- endset -%}
{{ return(query) }}

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
    case when ({{ r_val }}) = 0 then -(({{ pv_val }}) + ({{ fv_val }})) / nullif(({{ n_val }}), 0)
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
    {%- set vals = cols if cols else ['0'] -%}
    ( {% for c in vals %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), {{ loop.index }}){% endfor %} )
{%- elif fn == 'xnpv' -%}
    {%- set vals = cols if cols else ['0'] -%}
    ( {% for c in vals %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }}) / 365.0){% endfor %} )
{%- elif fn == 'fvschedule' -%}
    ({{ pr_val }}){% for c in cols %} * (1 + ({{ c | trim }})){% endfor %}
{%- elif fn == 'mirr' -%}
    {%- set vals = cols if (cols | length > 1) else ['0', '0'] -%}
    {%- set last = vals | length - 1 -%}
    power(
      -( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), {{ last - loop.index0 }}) else 0 end{% endfor %} )
      / nullif(( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), {{ loop.index0 }}) else 0 end{% endfor %} ), 0)
    , 1.0 / {{ last }}) - 1
{%- elif fn == 'mxirr' -%}
    {%- set vals = cols if (cols | length > 1) else ['0', '0'] -%}
    {%- set span = day_offsets[vals | length - 1] if day_offsets | length >= vals | length else '0' -%}
    power(
      -( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), (({{ span }}) - ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }})) / 365.0) else 0 end{% endfor %} )
      / nullif(( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }}) / 365.0) else 0 end{% endfor %} ), 0)
    , 365.0 / nullif(({{ span }}), 0)) - 1
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


{%- macro snowflake__Finance(
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
    n_iter
) %}

{%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
{%- set src = relation_list | join(', ') -%}
{%- set fn = (function_type | default('FV') | trim | lower) -%}
{%- set out_col = (output_column | default('finance_result') | trim) -%}

{# A blank input falls back to a neutral value so the expression still compiles. #}
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

{%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else [] -%}
{%- set dts  = date_list.split(',')  if (date_list  and date_list  | trim != '') else [] -%}

{# Whole days from the first date column to each date column, Snowflake style. #}
{%- set day_offsets = [] -%}
{%- for d in dts -%}
    {%- do day_offsets.append("datediff('day', cast(" ~ (dts[0] | trim) ~ " as date), cast(" ~ (d | trim) ~ " as date))") -%}
{%- endfor -%}

{%- if fn in ['irr', 'rate', 'xirr'] -%}

{%- set n_req = (n_iter | trim | int) if (n_iter and n_iter | trim != '') else 60 -%}
{%- set N = n_req if n_req > 0 else 60 -%}

{# The objective the solver drives to zero, rendered once per bracket variable. #}
{%- set obj = {} -%}
{%- for var in ['_fin_lo', '_fin_mid'] -%}
    {%- if fn == 'rate' -%}
        {%- do obj.update({var:
            '( case when (' ~ var ~ ') = 0 then (' ~ pv_val ~ ') + (' ~ p_val ~ ') * (' ~ n_val ~ ') + (' ~ fv_val ~ ')'
            ~ ' else (' ~ pv_val ~ ') * power(1 + (' ~ var ~ '), (' ~ n_val ~ '))'
            ~ ' + (' ~ p_val ~ ') * (1 + (' ~ var ~ ') * (' ~ type_val ~ '))'
            ~ ' * (power(1 + (' ~ var ~ '), (' ~ n_val ~ ')) - 1) / (' ~ var ~ ')'
            ~ ' + (' ~ fv_val ~ ') end )'}) -%}
    {%- else -%}
        {%- set parts = [] -%}
        {%- for c in cols -%}
            {%- if fn == 'irr' -%}
                {%- do parts.append('(' ~ (c | trim) ~ ') / power(1 + (' ~ var ~ '), ' ~ loop.index0 ~ ')') -%}
            {%- else -%}
                {%- set gap = day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' -%}
                {%- do parts.append('(' ~ (c | trim) ~ ') / power(1 + (' ~ var ~ '), (' ~ gap ~ ') / 365.0)') -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do obj.update({var: '( ' ~ (parts | join(' + ') if parts else '0') ~ ' )'}) -%}
    {%- endif -%}
{%- endfor -%}

{# The bracket and the objective at its lower end sit in separate CTEs on purpose:
   reading a SELECT-list alias from the same SELECT list is a lateral column alias,
   which not every warehouse accepts. #}
{%- set query -%}
with _fin_seed as (
    select *,
        ({{ lo_bound if (lo_bound and lo_bound | trim != '') else '-0.99' }}) as _fin_lo,
        ({{ hi_bound if (hi_bound and hi_bound | trim != '') else '10' }}) as _fin_hi
    from {{ src }}
)
, _fin_b0 as (
    select *, ({{ obj['_fin_lo'] }}) as _fin_flo
    from _fin_seed
)
{%- for i in range(N) %}
, _fin_p{{ i }} as (
    select *, ((_fin_lo + _fin_hi) / 2.0) as _fin_mid from _fin_b{{ i }}
)
, _fin_q{{ i }} as (
    select *, ({{ obj['_fin_mid'] }}) as _fin_fmid from _fin_p{{ i }}
)
, _fin_b{{ i + 1 }} as (
    select * EXCLUDE (_fin_lo, _fin_hi, _fin_flo, _fin_mid, _fin_fmid),
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_mid  else _fin_lo  end as _fin_lo,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_hi   else _fin_mid end as _fin_hi,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_fmid else _fin_flo end as _fin_flo
    from _fin_q{{ i }}
)
{%- endfor %}
select * EXCLUDE (_fin_lo, _fin_hi, _fin_flo),
    ((_fin_lo + _fin_hi) / 2.0) as {{ out_col }}
from _fin_b{{ N }}
{%- endset -%}
{{ return(query) }}

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
    case when ({{ r_val }}) = 0 then -(({{ pv_val }}) + ({{ fv_val }})) / nullif(({{ n_val }}), 0)
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
    {%- set vals = cols if cols else ['0'] -%}
    ( {% for c in vals %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), {{ loop.index }}){% endfor %} )
{%- elif fn == 'xnpv' -%}
    {%- set vals = cols if cols else ['0'] -%}
    ( {% for c in vals %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }}) / 365.0){% endfor %} )
{%- elif fn == 'fvschedule' -%}
    ({{ pr_val }}){% for c in cols %} * (1 + ({{ c | trim }})){% endfor %}
{%- elif fn == 'mirr' -%}
    {%- set vals = cols if (cols | length > 1) else ['0', '0'] -%}
    {%- set last = vals | length - 1 -%}
    power(
      -( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), {{ last - loop.index0 }}) else 0 end{% endfor %} )
      / nullif(( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), {{ loop.index0 }}) else 0 end{% endfor %} ), 0)
    , 1.0 / {{ last }}) - 1
{%- elif fn == 'mxirr' -%}
    {%- set vals = cols if (cols | length > 1) else ['0', '0'] -%}
    {%- set span = day_offsets[vals | length - 1] if day_offsets | length >= vals | length else '0' -%}
    power(
      -( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), (({{ span }}) - ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }})) / 365.0) else 0 end{% endfor %} )
      / nullif(( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }}) / 365.0) else 0 end{% endfor %} ), 0)
    , 365.0 / nullif(({{ span }}), 0)) - 1
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


{%- macro bigquery__Finance(
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
    n_iter
) %}

{%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
{%- set src = relation_list | join(', ') -%}
{%- set fn = (function_type | default('FV') | trim | lower) -%}
{%- set out_col = (output_column | default('finance_result') | trim) -%}

{# A blank input falls back to a neutral value so the expression still compiles. #}
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

{%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else [] -%}
{%- set dts  = date_list.split(',')  if (date_list  and date_list  | trim != '') else [] -%}

{# Whole days from the first date column to each date column, BigQuery style. #}
{%- set day_offsets = [] -%}
{%- for d in dts -%}
    {%- do day_offsets.append("date_diff(cast(" ~ (d | trim) ~ " as date), cast(" ~ (dts[0] | trim) ~ " as date), day)") -%}
{%- endfor -%}

{%- if fn in ['irr', 'rate', 'xirr'] -%}

{%- set n_req = (n_iter | trim | int) if (n_iter and n_iter | trim != '') else 60 -%}
{%- set N = n_req if n_req > 0 else 60 -%}

{# The objective the solver drives to zero, rendered once per bracket variable. #}
{%- set obj = {} -%}
{%- for var in ['_fin_lo', '_fin_mid'] -%}
    {%- if fn == 'rate' -%}
        {%- do obj.update({var:
            '( case when (' ~ var ~ ') = 0 then (' ~ pv_val ~ ') + (' ~ p_val ~ ') * (' ~ n_val ~ ') + (' ~ fv_val ~ ')'
            ~ ' else (' ~ pv_val ~ ') * power(1 + (' ~ var ~ '), (' ~ n_val ~ '))'
            ~ ' + (' ~ p_val ~ ') * (1 + (' ~ var ~ ') * (' ~ type_val ~ '))'
            ~ ' * (power(1 + (' ~ var ~ '), (' ~ n_val ~ ')) - 1) / (' ~ var ~ ')'
            ~ ' + (' ~ fv_val ~ ') end )'}) -%}
    {%- else -%}
        {%- set parts = [] -%}
        {%- for c in cols -%}
            {%- if fn == 'irr' -%}
                {%- do parts.append('(' ~ (c | trim) ~ ') / power(1 + (' ~ var ~ '), ' ~ loop.index0 ~ ')') -%}
            {%- else -%}
                {%- set gap = day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' -%}
                {%- do parts.append('(' ~ (c | trim) ~ ') / power(1 + (' ~ var ~ '), (' ~ gap ~ ') / 365.0)') -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do obj.update({var: '( ' ~ (parts | join(' + ') if parts else '0') ~ ' )'}) -%}
    {%- endif -%}
{%- endfor -%}

{# The bracket and the objective at its lower end sit in separate CTEs on purpose:
   reading a SELECT-list alias from the same SELECT list is a lateral column alias,
   which not every warehouse accepts. #}
{%- set query -%}
with _fin_seed as (
    select *,
        ({{ lo_bound if (lo_bound and lo_bound | trim != '') else '-0.99' }}) as _fin_lo,
        ({{ hi_bound if (hi_bound and hi_bound | trim != '') else '10' }}) as _fin_hi
    from {{ src }}
)
, _fin_b0 as (
    select *, ({{ obj['_fin_lo'] }}) as _fin_flo
    from _fin_seed
)
{%- for i in range(N) %}
, _fin_p{{ i }} as (
    select *, ((_fin_lo + _fin_hi) / 2.0) as _fin_mid from _fin_b{{ i }}
)
, _fin_q{{ i }} as (
    select *, ({{ obj['_fin_mid'] }}) as _fin_fmid from _fin_p{{ i }}
)
, _fin_b{{ i + 1 }} as (
    select * EXCEPT (_fin_lo, _fin_hi, _fin_flo, _fin_mid, _fin_fmid),
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_mid  else _fin_lo  end as _fin_lo,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_hi   else _fin_mid end as _fin_hi,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_fmid else _fin_flo end as _fin_flo
    from _fin_q{{ i }}
)
{%- endfor %}
select * EXCEPT (_fin_lo, _fin_hi, _fin_flo),
    ((_fin_lo + _fin_hi) / 2.0) as {{ out_col }}
from _fin_b{{ N }}
{%- endset -%}
{{ return(query) }}

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
    case when ({{ r_val }}) = 0 then -(({{ pv_val }}) + ({{ fv_val }})) / nullif(({{ n_val }}), 0)
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
    {%- set vals = cols if cols else ['0'] -%}
    ( {% for c in vals %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), {{ loop.index }}){% endfor %} )
{%- elif fn == 'xnpv' -%}
    {%- set vals = cols if cols else ['0'] -%}
    ( {% for c in vals %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }}) / 365.0){% endfor %} )
{%- elif fn == 'fvschedule' -%}
    ({{ pr_val }}){% for c in cols %} * (1 + ({{ c | trim }})){% endfor %}
{%- elif fn == 'mirr' -%}
    {%- set vals = cols if (cols | length > 1) else ['0', '0'] -%}
    {%- set last = vals | length - 1 -%}
    power(
      -( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), {{ last - loop.index0 }}) else 0 end{% endfor %} )
      / nullif(( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), {{ loop.index0 }}) else 0 end{% endfor %} ), 0)
    , 1.0 / {{ last }}) - 1
{%- elif fn == 'mxirr' -%}
    {%- set vals = cols if (cols | length > 1) else ['0', '0'] -%}
    {%- set span = day_offsets[vals | length - 1] if day_offsets | length >= vals | length else '0' -%}
    power(
      -( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), (({{ span }}) - ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }})) / 365.0) else 0 end{% endfor %} )
      / nullif(( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }}) / 365.0) else 0 end{% endfor %} ), 0)
    , 365.0 / nullif(({{ span }}), 0)) - 1
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


{%- macro duckdb__Finance(
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
    n_iter
) %}

{%- set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] -%}
{%- set src = relation_list | join(', ') -%}
{%- set fn = (function_type | default('FV') | trim | lower) -%}
{%- set out_col = (output_column | default('finance_result') | trim) -%}

{# A blank input falls back to a neutral value so the expression still compiles. #}
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

{%- set cols = value_list.split(',') if (value_list and value_list | trim != '') else [] -%}
{%- set dts  = date_list.split(',')  if (date_list  and date_list  | trim != '') else [] -%}

{# Whole days from the first date column to each date column, DuckDB style. #}
{%- set day_offsets = [] -%}
{%- for d in dts -%}
    {%- do day_offsets.append("date_diff('day', cast(" ~ (dts[0] | trim) ~ " as date), cast(" ~ (d | trim) ~ " as date))") -%}
{%- endfor -%}

{%- if fn in ['irr', 'rate', 'xirr'] -%}

{%- set n_req = (n_iter | trim | int) if (n_iter and n_iter | trim != '') else 60 -%}
{%- set N = n_req if n_req > 0 else 60 -%}

{# The objective the solver drives to zero, rendered once per bracket variable. #}
{%- set obj = {} -%}
{%- for var in ['_fin_lo', '_fin_mid'] -%}
    {%- if fn == 'rate' -%}
        {%- do obj.update({var:
            '( case when (' ~ var ~ ') = 0 then (' ~ pv_val ~ ') + (' ~ p_val ~ ') * (' ~ n_val ~ ') + (' ~ fv_val ~ ')'
            ~ ' else (' ~ pv_val ~ ') * power(1 + (' ~ var ~ '), (' ~ n_val ~ '))'
            ~ ' + (' ~ p_val ~ ') * (1 + (' ~ var ~ ') * (' ~ type_val ~ '))'
            ~ ' * (power(1 + (' ~ var ~ '), (' ~ n_val ~ ')) - 1) / (' ~ var ~ ')'
            ~ ' + (' ~ fv_val ~ ') end )'}) -%}
    {%- else -%}
        {%- set parts = [] -%}
        {%- for c in cols -%}
            {%- if fn == 'irr' -%}
                {%- do parts.append('(' ~ (c | trim) ~ ') / power(1 + (' ~ var ~ '), ' ~ loop.index0 ~ ')') -%}
            {%- else -%}
                {%- set gap = day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' -%}
                {%- do parts.append('(' ~ (c | trim) ~ ') / power(1 + (' ~ var ~ '), (' ~ gap ~ ') / 365.0)') -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do obj.update({var: '( ' ~ (parts | join(' + ') if parts else '0') ~ ' )'}) -%}
    {%- endif -%}
{%- endfor -%}

{# The bracket and the objective at its lower end sit in separate CTEs on purpose:
   reading a SELECT-list alias from the same SELECT list is a lateral column alias,
   which not every warehouse accepts. #}
{%- set query -%}
with _fin_seed as (
    select *,
        ({{ lo_bound if (lo_bound and lo_bound | trim != '') else '-0.99' }}) as _fin_lo,
        ({{ hi_bound if (hi_bound and hi_bound | trim != '') else '10' }}) as _fin_hi
    from {{ src }}
)
, _fin_b0 as (
    select *, ({{ obj['_fin_lo'] }}) as _fin_flo
    from _fin_seed
)
{%- for i in range(N) %}
, _fin_p{{ i }} as (
    select *, ((_fin_lo + _fin_hi) / 2.0) as _fin_mid from _fin_b{{ i }}
)
, _fin_q{{ i }} as (
    select *, ({{ obj['_fin_mid'] }}) as _fin_fmid from _fin_p{{ i }}
)
, _fin_b{{ i + 1 }} as (
    select * EXCLUDE (_fin_lo, _fin_hi, _fin_flo, _fin_mid, _fin_fmid),
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_mid  else _fin_lo  end as _fin_lo,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_hi   else _fin_mid end as _fin_hi,
        case when sign(_fin_fmid) = sign(_fin_flo) then _fin_fmid else _fin_flo end as _fin_flo
    from _fin_q{{ i }}
)
{%- endfor %}
select * EXCLUDE (_fin_lo, _fin_hi, _fin_flo),
    ((_fin_lo + _fin_hi) / 2.0) as {{ out_col }}
from _fin_b{{ N }}
{%- endset -%}
{{ return(query) }}

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
    case when ({{ r_val }}) = 0 then -(({{ pv_val }}) + ({{ fv_val }})) / nullif(({{ n_val }}), 0)
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
    {%- set vals = cols if cols else ['0'] -%}
    ( {% for c in vals %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), {{ loop.index }}){% endfor %} )
{%- elif fn == 'xnpv' -%}
    {%- set vals = cols if cols else ['0'] -%}
    ( {% for c in vals %}{{ ' + ' if not loop.first else '' }}({{ c | trim }}) / power(1 + ({{ r_val }}), ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }}) / 365.0){% endfor %} )
{%- elif fn == 'fvschedule' -%}
    ({{ pr_val }}){% for c in cols %} * (1 + ({{ c | trim }})){% endfor %}
{%- elif fn == 'mirr' -%}
    {%- set vals = cols if (cols | length > 1) else ['0', '0'] -%}
    {%- set last = vals | length - 1 -%}
    power(
      -( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), {{ last - loop.index0 }}) else 0 end{% endfor %} )
      / nullif(( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), {{ loop.index0 }}) else 0 end{% endfor %} ), 0)
    , 1.0 / {{ last }}) - 1
{%- elif fn == 'mxirr' -%}
    {%- set vals = cols if (cols | length > 1) else ['0', '0'] -%}
    {%- set span = day_offsets[vals | length - 1] if day_offsets | length >= vals | length else '0' -%}
    power(
      -( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) > 0 then ({{ c | trim }}) * power(1 + ({{ rr_val }}), (({{ span }}) - ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }})) / 365.0) else 0 end{% endfor %} )
      / nullif(( {% for c in vals %}{{ ' + ' if not loop.first else '' }}case when ({{ c | trim }}) < 0 then ({{ c | trim }}) / power(1 + ({{ fr_val }}), ({{ day_offsets[loop.index0] if day_offsets | length > loop.index0 else '0' }}) / 365.0) else 0 end{% endfor %} ), 0)
    , 365.0 / nullif(({{ span }}), 0)) - 1
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
