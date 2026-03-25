{% macro Imputation(
    relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField
) -%}
    {{ return(adapter.dispatch('Imputation', 'prophecy_basics')(
        relation_name,
        schema,
        columnNames,
        replaceIncomingType,
        incomingUserValue,
        replaceWithType,
        replaceWithUserValue,
        includeImputedIndicator,
        outputImputedAsSeparateField
    )) }}
{% endmacro %}


{# ============================================================
   SPARK / DATABRICKS  (default__)
   - Quoting : backticks
   - Median  : PERCENTILE_APPROX(..., 0.5)
   - Mode    : inline ORDER BY / LIMIT subquery
   ============================================================ #}
{% macro default__Imputation(
    relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField
) %}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{% set bt = '`' %}

{% set _imp = prophecy_basics.normalize_imputation_args(schema, columnNames) %}
{% set schema = _imp[0] %}
{% set columnNames = _imp[1] %}

{# Short-circuit: nothing to impute #}
{% if columnNames | length == 0 %}
    SELECT * FROM {{ relation_list | join(', ') }}
{% else %}

{# Build a name→dataType lookup #}
{% set col_type_map = {} %}
{% for c in schema %}
    {% do col_type_map.update({ c.name: (c.dataType | lower) }) %}
{% endfor %}

WITH base AS (
    SELECT * FROM {{ relation_list | join(', ') }}
)

{# ── Per-column stat CTEs ── #}
{% for col_name in columnNames %}
    {% set col_type = col_type_map.get(col_name, 'string') %}
    {% set qcol = bt ~ col_name ~ bt %}

    {# Change 2: proper literal quoting #}
    {% if prophecy_basics.is_string_type(col_type) == 'true' %}
        {% set incoming_lit = prophecy_basics.cast_timestamp_if_needed(incomingUserValue) %}
        {% set user_lit = prophecy_basics.cast_timestamp_if_needed(replaceWithUserValue) %}
    {% else %}
        {% set incoming_lit = incomingUserValue | string %}
        {% set user_lit = replaceWithUserValue | string %}
    {% endif %}

    {# Condition: which rows to EXCLUDE from the aggregate #}
    {% if replaceIncomingType == 'null_val' %}
        {% set filter_agg = qcol ~ ' IS NOT NULL' %}
    {% else %}
        {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ incoming_lit ~ ')' %}
    {% endif %}

    {# Change 4: aggregate expression with CAST for integer types #}
    {% if replaceWithType == 'average' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END) AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% endif %}
    {% elif replaceWithType == 'median' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(PERCENTILE_APPROX(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END, 0.5) AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'PERCENTILE_APPROX(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END, 0.5)' %}
        {% endif %}
    {% elif replaceWithType == 'mode' %}
        {# Change 4: CAST mode select col for integer types; Change 5: deterministic tie-breaking #}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set mode_select = 'CAST(' ~ qcol ~ ' AS BIGINT)' %}
        {% else %}
            {% set mode_select = qcol %}
        {% endif %}
        {% set rep_expr = '(' ~
            'SELECT ' ~ mode_select ~
            ' FROM base' ~
            ' WHERE ' ~ filter_agg ~
            ' GROUP BY ' ~ qcol ~
            ' ORDER BY COUNT(*) DESC, ' ~ qcol ~ ' ASC' ~
            ' LIMIT 1)' %}
    {% else %}
        {# replaceWithType == 'user'; Change 4: CAST for integer types #}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(' ~ user_lit ~ ' AS BIGINT)' %}
        {% else %}
            {% set rep_expr = user_lit %}
        {% endif %}
    {% endif %}

    , stat_{{ loop.index0 }} AS (
        {% if replaceWithType in ('user', 'mode') %}
            SELECT {{ rep_expr }} AS rep_val
        {% else %}
            SELECT {{ rep_expr }} AS rep_val FROM base
        {% endif %}
    )
{% endfor %}

{# ── Final SELECT — flat list approach (Change 7) ── #}
{% set select_cols = [] %}
{% for c in schema %}
    {% set col_name = c.name %}
    {% set col_type = col_type_map.get(col_name, '') %}
    {% set quoted_col = bt ~ col_name ~ bt %}

    {# Change 2: quoting for is_imputed_cond in final SELECT #}
    {% if prophecy_basics.is_string_type(col_type) == 'true' %}
        {% set incoming_lit = prophecy_basics.cast_timestamp_if_needed(incomingUserValue) %}
    {% else %}
        {% set incoming_lit = incomingUserValue | string %}
    {% endif %}

    {# Use namespace so impute_idx assignment inside the loop escapes loop scope #}
    {% set ns = namespace(impute_idx=-1) %}
    {% for impute_col in columnNames %}
        {% if (impute_col | upper) == (col_name | upper) %}
            {% set ns.impute_idx = loop.index0 %}
        {% endif %}
    {% endfor %}

    {% if ns.impute_idx == -1 %}
        {% do select_cols.append(quoted_col) %}
    {% else %}
        {% set qcol = quoted_col %}
        {% set quoted_imputed_col = bt ~ col_name ~ '_ImputedValue' ~ bt %}
        {% set quoted_indicator_col = bt ~ col_name ~ '_Indicator' ~ bt %}

        {# Change 6: inline scalar subqueries, no stats CTE reference #}
        {% if replaceIncomingType == 'null_val' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
            {% set replace_expr = 'COALESCE(' ~ qcol ~ ', (SELECT rep_val FROM stat_' ~ ns.impute_idx ~ '))' %}
        {% else %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ incoming_lit %}
            {% set replace_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN (SELECT rep_val FROM stat_' ~ ns.impute_idx ~ ') ELSE ' ~ qcol ~ ' END' %}
        {% endif %}

        {% if outputImputedAsSeparateField %}
            {% do select_cols.append(qcol) %}
            {% do select_cols.append(replace_expr ~ ' AS ' ~ quoted_imputed_col) %}
        {% else %}
            {% do select_cols.append(replace_expr ~ ' AS ' ~ qcol) %}
        {% endif %}
        {% if includeImputedIndicator %}
            {% set indicator_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN 1 ELSE 0 END' %}
            {% do select_cols.append(indicator_expr ~ ' AS ' ~ quoted_indicator_col) %}
        {% endif %}
    {% endif %}
{% endfor %}

SELECT
    {{ select_cols | join(',\n    ') }}
FROM base

{% endif %}
{% endmacro %}


{# ============================================================
   DUCKDB  (duckdb__)
   - Quoting  : double-quotes via prophecy_basics.quote_identifier
   - Median   : MEDIAN() native aggregate
   - Mode     : MODE() native aggregate
   - Filter   : <> instead of !=
   ============================================================ #}
{% macro duckdb__Imputation(
    relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField
) %}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

{% set _imp = prophecy_basics.normalize_imputation_args(schema, columnNames) %}
{% set schema = _imp[0] %}
{% set columnNames = _imp[1] %}

{% if columnNames | length == 0 %}
    SELECT * FROM {{ relation_list | join(', ') }}
{% else %}

{% set col_type_map = {} %}
{% for c in schema %}
    {% do col_type_map.update({ c.name: (c.dataType | lower) }) %}
{% endfor %}

WITH base AS (
    SELECT * FROM {{ relation_list | join(', ') }}
)

{% for col_name in columnNames %}
    {% set col_type = col_type_map.get(col_name, 'string') %}
    {% set qcol = prophecy_basics.quote_identifier(col_name) %}

    {# Change 2: proper literal quoting #}
    {% if prophecy_basics.is_string_type(col_type) == 'true' %}
        {% set incoming_lit = prophecy_basics.cast_timestamp_if_needed(incomingUserValue) %}
        {% set user_lit = prophecy_basics.cast_timestamp_if_needed(replaceWithUserValue) %}
    {% else %}
        {% set incoming_lit = incomingUserValue | string %}
        {% set user_lit = replaceWithUserValue | string %}
    {% endif %}

    {# DuckDB keeps <> for filter condition #}
    {% if replaceIncomingType == 'null_val' %}
        {% set filter_agg = qcol ~ ' IS NOT NULL' %}
    {% else %}
        {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' <> ' ~ incoming_lit ~ ')' %}
    {% endif %}

    {# Change 4: aggregate expression with CAST for integer types #}
    {# DuckDB keeps MODE() and MEDIAN() as native aggregates (no subquery / ORDER BY tie-break) #}
    {% if replaceWithType == 'average' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END) AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% endif %}
    {% elif replaceWithType == 'median' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(MEDIAN(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END) AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'MEDIAN(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% endif %}
    {% elif replaceWithType == 'mode' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(MODE(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END) AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'MODE(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% endif %}
    {% else %}
        {# replaceWithType == 'user'; Change 4: CAST for integer types #}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(' ~ user_lit ~ ' AS BIGINT)' %}
        {% else %}
            {% set rep_expr = user_lit %}
        {% endif %}
    {% endif %}

    , stat_{{ loop.index0 }} AS (
        {% if replaceWithType == 'user' %}
            SELECT {{ rep_expr }} AS rep_val
        {% else %}
            SELECT {{ rep_expr }} AS rep_val FROM base
        {% endif %}
    )
{% endfor %}

{# ── Final SELECT — flat list approach (Change 7) ── #}
{% set select_cols = [] %}
{% for c in schema %}
    {% set col_name = c.name %}
    {% set col_type = col_type_map.get(col_name, '') %}
    {% set quoted_col = prophecy_basics.quote_identifier(col_name) %}

    {# Change 2: quoting for is_imputed_cond in final SELECT #}
    {% if prophecy_basics.is_string_type(col_type) == 'true' %}
        {% set incoming_lit = prophecy_basics.cast_timestamp_if_needed(incomingUserValue) %}
    {% else %}
        {% set incoming_lit = incomingUserValue | string %}
    {% endif %}

    {# Use namespace so impute_idx assignment inside the loop escapes loop scope #}
    {% set ns = namespace(impute_idx=-1) %}
    {% for impute_col in columnNames %}
        {% if (impute_col | upper) == (col_name | upper) %}
            {% set ns.impute_idx = loop.index0 %}
        {% endif %}
    {% endfor %}

    {% if ns.impute_idx == -1 %}
        {% do select_cols.append(quoted_col) %}
    {% else %}
        {% set qcol = quoted_col %}
        {% set quoted_imputed_col = prophecy_basics.quote_identifier(col_name ~ '_ImputedValue') %}
        {% set quoted_indicator_col = prophecy_basics.quote_identifier(col_name ~ '_Indicator') %}

        {# Change 6: inline scalar subqueries, no stats CTE reference #}
        {# DuckDB keeps <> in is_imputed_cond filter #}
        {% if replaceIncomingType == 'null_val' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
            {% set replace_expr = 'COALESCE(' ~ qcol ~ ', (SELECT rep_val FROM stat_' ~ ns.impute_idx ~ '))' %}
        {% else %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ incoming_lit %}
            {% set replace_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN (SELECT rep_val FROM stat_' ~ ns.impute_idx ~ ') ELSE ' ~ qcol ~ ' END' %}
        {% endif %}

        {% if outputImputedAsSeparateField %}
            {% do select_cols.append(qcol) %}
            {% do select_cols.append(replace_expr ~ ' AS ' ~ quoted_imputed_col) %}
        {% else %}
            {% do select_cols.append(replace_expr ~ ' AS ' ~ qcol) %}
        {% endif %}
        {% if includeImputedIndicator %}
            {% set indicator_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN 1 ELSE 0 END' %}
            {% do select_cols.append(indicator_expr ~ ' AS ' ~ quoted_indicator_col) %}
        {% endif %}
    {% endif %}
{% endfor %}

SELECT
    {{ select_cols | join(',\n    ') }}
FROM base

{% endif %}
{% endmacro %}


{# ============================================================
   BIGQUERY  (bigquery__)
   - Quoting  : backticks
   - Median   : APPROX_QUANTILES(..., 100)[OFFSET(50)]
   - Mode     : ORDER BY COUNT(*) DESC, col ASC LIMIT 1 subquery
   - Indicator: IF() instead of CASE WHEN
   ============================================================ #}
{% macro bigquery__Imputation(
    relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField
) %}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
{% set bt = '`' %}

{% set _imp = prophecy_basics.normalize_imputation_args(schema, columnNames) %}
{% set schema = _imp[0] %}
{% set columnNames = _imp[1] %}

{% if columnNames | length == 0 %}
    SELECT * FROM {{ relation_list | join(', ') }}
{% else %}

{% set col_type_map = {} %}
{% for c in schema %}
    {% do col_type_map.update({ c.name: (c.dataType | lower) }) %}
{% endfor %}

WITH base AS (
    SELECT * FROM {{ relation_list | join(', ') }}
)

{% for col_name in columnNames %}
    {% set col_type = col_type_map.get(col_name, 'string') %}
    {% set qcol = bt ~ col_name ~ bt %}

    {# Change 2: proper literal quoting #}
    {% if prophecy_basics.is_string_type(col_type) == 'true' %}
        {% set incoming_lit = prophecy_basics.cast_timestamp_if_needed(incomingUserValue) %}
        {% set user_lit = prophecy_basics.cast_timestamp_if_needed(replaceWithUserValue) %}
    {% else %}
        {% set incoming_lit = incomingUserValue | string %}
        {% set user_lit = replaceWithUserValue | string %}
    {% endif %}

    {% if replaceIncomingType == 'null_val' %}
        {% set filter_agg = qcol ~ ' IS NOT NULL' %}
    {% else %}
        {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ incoming_lit ~ ')' %}
    {% endif %}

    {# Change 4: aggregate expression with CAST for integer types #}
    {% if replaceWithType == 'average' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END) AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% endif %}
    {% elif replaceWithType == 'median' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(APPROX_QUANTILES(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END, 100)[OFFSET(50)] AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'APPROX_QUANTILES(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END, 100)[OFFSET(50)]' %}
        {% endif %}
    {% elif replaceWithType == 'mode' %}
        {# Change 4: CAST mode select col for integer types; Change 5: deterministic tie-breaking #}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set mode_select = 'CAST(' ~ qcol ~ ' AS BIGINT)' %}
        {% else %}
            {% set mode_select = qcol %}
        {% endif %}
        {% set rep_expr = '(' ~
            'SELECT ' ~ mode_select ~
            ' FROM base' ~
            ' WHERE ' ~ filter_agg ~
            ' GROUP BY ' ~ qcol ~
            ' ORDER BY COUNT(*) DESC, ' ~ qcol ~ ' ASC' ~
            ' LIMIT 1)' %}
    {% else %}
        {# replaceWithType == 'user'; Change 4: CAST for integer types #}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(' ~ user_lit ~ ' AS BIGINT)' %}
        {% else %}
            {% set rep_expr = user_lit %}
        {% endif %}
    {% endif %}

    , stat_{{ loop.index0 }} AS (
        {% if replaceWithType in ('user', 'mode') %}
            SELECT {{ rep_expr }} AS rep_val
        {% else %}
            SELECT {{ rep_expr }} AS rep_val FROM base
        {% endif %}
    )
{% endfor %}

{# ── Final SELECT — flat list approach (Change 7) ── #}
{% set select_cols = [] %}
{% for c in schema %}
    {% set col_name = c.name %}
    {% set col_type = col_type_map.get(col_name, '') %}
    {% set quoted_col = bt ~ col_name ~ bt %}

    {# Change 2: quoting for is_imputed_cond in final SELECT #}
    {% if prophecy_basics.is_string_type(col_type) == 'true' %}
        {% set incoming_lit = prophecy_basics.cast_timestamp_if_needed(incomingUserValue) %}
    {% else %}
        {% set incoming_lit = incomingUserValue | string %}
    {% endif %}

    {# Use namespace so impute_idx assignment inside the loop escapes loop scope #}
    {% set ns = namespace(impute_idx=-1) %}
    {% for impute_col in columnNames %}
        {% if (impute_col | upper) == (col_name | upper) %}
            {% set ns.impute_idx = loop.index0 %}
        {% endif %}
    {% endfor %}

    {% if ns.impute_idx == -1 %}
        {% do select_cols.append(quoted_col) %}
    {% else %}
        {% set qcol = quoted_col %}
        {% set quoted_imputed_col = bt ~ col_name ~ '_ImputedValue' ~ bt %}
        {% set quoted_indicator_col = bt ~ col_name ~ '_Indicator' ~ bt %}

        {# Change 6: inline scalar subqueries, no stats CTE reference #}
        {% if replaceIncomingType == 'null_val' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
            {% set replace_expr = 'COALESCE(' ~ qcol ~ ', (SELECT rep_val FROM stat_' ~ ns.impute_idx ~ '))' %}
        {% else %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ incoming_lit %}
            {% set replace_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN (SELECT rep_val FROM stat_' ~ ns.impute_idx ~ ') ELSE ' ~ qcol ~ ' END' %}
        {% endif %}

        {% if outputImputedAsSeparateField %}
            {% do select_cols.append(qcol) %}
            {% do select_cols.append(replace_expr ~ ' AS ' ~ quoted_imputed_col) %}
        {% else %}
            {% do select_cols.append(replace_expr ~ ' AS ' ~ qcol) %}
        {% endif %}
        {% if includeImputedIndicator %}
            {# BigQuery keeps IF() for the indicator #}
            {% set indicator_expr = 'IF(' ~ is_imputed_cond ~ ', 1, 0)' %}
            {% do select_cols.append(indicator_expr ~ ' AS ' ~ quoted_indicator_col) %}
        {% endif %}
    {% endif %}
{% endfor %}

SELECT
    {{ select_cols | join(',\n    ') }}
FROM base

{% endif %}
{% endmacro %}


{# ============================================================
   SNOWFLAKE  (snowflake__)
   - Quoting  : double-quotes via prophecy_basics.quote_identifier
   - Median   : MEDIAN() native aggregate
   - Mode     : ORDER BY COUNT(*) DESC, col ASC LIMIT 1 subquery
                (Snowflake has no MODE() aggregate)
   ============================================================ #}
{% macro snowflake__Imputation(
    relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField
) %}

{% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}

{% set _imp = prophecy_basics.normalize_imputation_args(schema, columnNames) %}
{% set schema = _imp[0] %}
{% set columnNames = _imp[1] %}

{% if columnNames | length == 0 %}
    SELECT * FROM {{ relation_list | join(', ') }}
{% else %}

{% set col_type_map = {} %}
{% for c in schema %}
    {% do col_type_map.update({ c.name: (c.dataType | lower) }) %}
{% endfor %}

WITH base AS (
    SELECT * FROM {{ relation_list | join(', ') }}
)

{% for col_name in columnNames %}
    {% set col_type = col_type_map.get(col_name, 'string') %}
    {% set qcol = prophecy_basics.quote_identifier(col_name) %}

    {# Change 2: proper literal quoting #}
    {% if prophecy_basics.is_string_type(col_type) == 'true' %}
        {% set incoming_lit = prophecy_basics.cast_timestamp_if_needed(incomingUserValue) %}
        {% set user_lit = prophecy_basics.cast_timestamp_if_needed(replaceWithUserValue) %}
    {% else %}
        {% set incoming_lit = incomingUserValue | string %}
        {% set user_lit = replaceWithUserValue | string %}
    {% endif %}

    {% if replaceIncomingType == 'null_val' %}
        {% set filter_agg = qcol ~ ' IS NOT NULL' %}
    {% else %}
        {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ incoming_lit ~ ')' %}
    {% endif %}

    {# Change 4: aggregate expression with CAST for integer types #}
    {# Snowflake keeps MEDIAN() as a native aggregate #}
    {% if replaceWithType == 'average' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END) AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% endif %}
    {% elif replaceWithType == 'median' %}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(MEDIAN(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END) AS BIGINT)' %}
        {% else %}
            {% set rep_expr = 'MEDIAN(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% endif %}
    {% elif replaceWithType == 'mode' %}
        {# Change 4: CAST mode select col for integer types; Change 5: deterministic tie-breaking #}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set mode_select = 'CAST(' ~ qcol ~ ' AS BIGINT)' %}
        {% else %}
            {% set mode_select = qcol %}
        {% endif %}
        {% set rep_expr = '(' ~
            'SELECT ' ~ mode_select ~
            ' FROM base' ~
            ' WHERE ' ~ filter_agg ~
            ' GROUP BY ' ~ qcol ~
            ' ORDER BY COUNT(*) DESC, ' ~ qcol ~ ' ASC' ~
            ' LIMIT 1)' %}
    {% else %}
        {# replaceWithType == 'user'; Change 4: CAST for integer types #}
        {% if prophecy_basics.is_integer_type(col_type) == 'true' %}
            {% set rep_expr = 'CAST(' ~ user_lit ~ ' AS BIGINT)' %}
        {% else %}
            {% set rep_expr = user_lit %}
        {% endif %}
    {% endif %}

    , stat_{{ loop.index0 }} AS (
        {% if replaceWithType in ('user', 'mode') %}
            SELECT {{ rep_expr }} AS rep_val
        {% else %}
            SELECT {{ rep_expr }} AS rep_val FROM base
        {% endif %}
    )
{% endfor %}

{# ── Final SELECT — flat list approach (Change 7) ── #}
{% set select_cols = [] %}
{% for c in schema %}
    {% set col_name = c.name %}
    {% set col_type = col_type_map.get(col_name, '') %}
    {% set quoted_col = prophecy_basics.quote_identifier(col_name) %}

    {# Change 2: quoting for is_imputed_cond in final SELECT #}
    {% if prophecy_basics.is_string_type(col_type) == 'true' %}
        {% set incoming_lit = prophecy_basics.cast_timestamp_if_needed(incomingUserValue) %}
    {% else %}
        {% set incoming_lit = incomingUserValue | string %}
    {% endif %}

    {# Use namespace so impute_idx assignment inside the loop escapes loop scope #}
    {% set ns = namespace(impute_idx=-1) %}
    {% for impute_col in columnNames %}
        {% if (impute_col | upper) == (col_name | upper) %}
            {% set ns.impute_idx = loop.index0 %}
        {% endif %}
    {% endfor %}

    {% if ns.impute_idx == -1 %}
        {% do select_cols.append(quoted_col) %}
    {% else %}
        {% set qcol = quoted_col %}
        {% set quoted_imputed_col = prophecy_basics.quote_identifier(col_name ~ '_ImputedValue') %}
        {% set quoted_indicator_col = prophecy_basics.quote_identifier(col_name ~ '_Indicator') %}

        {# Change 6: inline scalar subqueries, no stats CTE reference #}
        {% if replaceIncomingType == 'null_val' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
            {% set replace_expr = 'COALESCE(' ~ qcol ~ ', (SELECT rep_val FROM stat_' ~ ns.impute_idx ~ '))' %}
        {% else %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ incoming_lit %}
            {% set replace_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN (SELECT rep_val FROM stat_' ~ ns.impute_idx ~ ') ELSE ' ~ qcol ~ ' END' %}
        {% endif %}

        {% if outputImputedAsSeparateField %}
            {% do select_cols.append(qcol) %}
            {% do select_cols.append(replace_expr ~ ' AS ' ~ quoted_imputed_col) %}
        {% else %}
            {% do select_cols.append(replace_expr ~ ' AS ' ~ qcol) %}
        {% endif %}
        {% if includeImputedIndicator %}
            {% set indicator_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN 1 ELSE 0 END' %}
            {% do select_cols.append(indicator_expr ~ ' AS ' ~ quoted_indicator_col) %}
        {% endif %}
    {% endif %}
{% endfor %}

SELECT
    {{ select_cols | join(',\n    ') }}
FROM base

{% endif %}
{% endmacro %}