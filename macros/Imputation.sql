{% macro Imputation(relation_name,
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
    {% set qcol = bt ~ col_name ~ bt %}

    {# Condition: which rows to EXCLUDE from the aggregate #}
    {% if replaceIncomingType == 'null_val' %}
        {% set filter_agg = qcol ~ ' IS NOT NULL' %}
    {% else %}
        {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ (incomingUserValue | string) ~ ')' %}
    {% endif %}

    {# Aggregate expression for the replacement value #}
    {% if replaceWithType == 'average' %}
        {% set rep_expr %}AVG(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END){% endset %}
    {% elif replaceWithType == 'median' %}
        {% set rep_expr %}PERCENTILE_APPROX(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END, 0.5){% endset %}
    {% elif replaceWithType == 'mode' %}
        {#
          Mode via a subquery that returns the most-frequent non-excluded value.
          We SELECT the value directly (no table-alias column reference) to avoid
          the broken `_m.`col`` pattern.
        #}
        {% set rep_expr %}(
            SELECT {{ qcol }}
            FROM base
            WHERE {{ filter_agg }}
            GROUP BY {{ qcol }}
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ){% endset %}
    {% else %}
        {# replaceWithType == 'user' #}
        {% set rep_expr = replaceWithUserValue | string %}
    {% endif %}

    , stat_{{ loop.index0 }} AS (
        {% if replaceWithType == 'user' %}
            SELECT {{ rep_expr }} AS rep_val
        {% elif replaceWithType == 'mode' %}
            {# Mode subquery is already scalar; wrap it #}
            SELECT {{ rep_expr }} AS rep_val
        {% else %}
            SELECT {{ rep_expr }} AS rep_val FROM base
        {% endif %}
    )
{% endfor %}

{# ── Consolidate all stats into one row ── #}
, stats AS (
    SELECT
    {% for col_name in columnNames %}
        (SELECT rep_val FROM stat_{{ loop.index0 }}) AS rep_{{ loop.index0 }}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
)

{# ── Final SELECT ── #}
SELECT
{% for c in schema %}
    {% set col_name = c.name %}
    {% set key = col_name | upper %}

    {# Resolve whether this column is in the impute list #}
    {% set in_impute_list = false %}
    {% set impute_idx = -1 %}
    {% for impute_col in columnNames %}
        {% if (impute_col | upper) == key %}
            {% set in_impute_list = true %}
            {% set impute_idx = loop.index0 %}
        {% endif %}
    {% endfor %}

    {% if not in_impute_list %}
        {# Pass-through column #}
        {{ bt ~ col_name ~ bt }}
    {% else %}
        {% set qcol = bt ~ col_name ~ bt %}

        {# Condition that identifies a row as "needs imputation" #}
        {% if replaceIncomingType == 'null_val' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
        {% else %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
        {% endif %}

        {# Replacement expression — simplified COALESCE for null_val, CASE for user #}
        {% if replaceIncomingType == 'null_val' %}
            {% set replace_expr = 'COALESCE(' ~ qcol ~ ', (SELECT rep_' ~ impute_idx ~ ' FROM stats))' %}
        {% else %}
            {% set replace_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN (SELECT rep_' ~ impute_idx ~ ' FROM stats) ELSE ' ~ qcol ~ ' END' %}
        {% endif %}

        {% if outputImputedAsSeparateField %}
            {{ qcol }},
            {{ replace_expr }} AS {{ bt ~ col_name ~ '_ImputedValue' ~ bt }}
            {% if includeImputedIndicator %},
            CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ bt ~ col_name ~ '_Indicator' ~ bt }}
            {% endif %}
        {% else %}
            {{ replace_expr }} AS {{ qcol }}
            {% if includeImputedIndicator %},
            CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ bt ~ col_name ~ '_Indicator' ~ bt }}
            {% endif %}
        {% endif %}
    {% endif %}
    {%- if not loop.last %},{% endif %}
{% endfor %}
FROM base
CROSS JOIN stats

{% endif %}
{% endmacro %}


{# ============================================================
   DUCKDB  (duckdb__)
   - Quoting  : double-quotes via prophecy_basics.quote_identifier
   - Median   : MEDIAN() native aggregate
   - Mode     : MODE() native aggregate
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
    {% set qcol = prophecy_basics.quote_identifier(col_name) %}

    {% if replaceIncomingType == 'null_val' %}
        {% set filter_agg = qcol ~ ' IS NOT NULL' %}
    {% else %}
        {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' <> ' ~ (incomingUserValue | string) ~ ')' %}
    {% endif %}

    {% if replaceWithType == 'average' %}
        {% set rep_expr %}AVG(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END){% endset %}
    {% elif replaceWithType == 'median' %}
        {% set rep_expr %}MEDIAN(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END){% endset %}
    {% elif replaceWithType == 'mode' %}
        {% set rep_expr %}MODE(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END){% endset %}
    {% else %}
        {% set rep_expr = replaceWithUserValue | string %}
    {% endif %}

    , stat_{{ loop.index0 }} AS (
        {% if replaceWithType == 'user' %}
            SELECT {{ rep_expr }} AS rep_val
        {% else %}
            SELECT {{ rep_expr }} AS rep_val FROM base
        {% endif %}
    )
{% endfor %}

, stats AS (
    SELECT
    {% for col_name in columnNames %}
        (SELECT rep_val FROM stat_{{ loop.index0 }}) AS rep_{{ loop.index0 }}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
)

SELECT
{% for c in schema %}
    {% set col_name = c.name %}
    {% set key = col_name | upper %}
    {% set in_impute_list = false %}
    {% set impute_idx = -1 %}
    {% for impute_col in columnNames %}
        {% if (impute_col | upper) == key %}
            {% set in_impute_list = true %}
            {% set impute_idx = loop.index0 %}
        {% endif %}
    {% endfor %}

    {% if not in_impute_list %}
        {{ prophecy_basics.quote_identifier(col_name) }}
    {% else %}
        {% set qcol = prophecy_basics.quote_identifier(col_name) %}

        {% if replaceIncomingType == 'null_val' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
            {% set replace_expr = 'COALESCE(' ~ qcol ~ ', (SELECT rep_' ~ impute_idx ~ ' FROM stats))' %}
        {% else %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
            {% set replace_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN (SELECT rep_' ~ impute_idx ~ ' FROM stats) ELSE ' ~ qcol ~ ' END' %}
        {% endif %}

        {% if outputImputedAsSeparateField %}
            {{ qcol }},
            {{ replace_expr }} AS {{ prophecy_basics.quote_identifier(col_name ~ '_ImputedValue') }}
            {% if includeImputedIndicator %},
            CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}
            {% endif %}
        {% else %}
            {{ replace_expr }} AS {{ qcol }}
            {% if includeImputedIndicator %},
            CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}
            {% endif %}
        {% endif %}
    {% endif %}
    {%- if not loop.last %},{% endif %}
{% endfor %}
FROM base
CROSS JOIN stats

{% endif %}
{% endmacro %}


{# ============================================================
   BIGQUERY  (bigquery__)
   - Quoting  : backticks
   - Median   : APPROX_QUANTILES(..., 100)[OFFSET(50)]
   - Mode     : ORDER BY COUNT(*) DESC LIMIT 1 subquery
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
    {% set qcol = bt ~ col_name ~ bt %}

    {% if replaceIncomingType == 'null_val' %}
        {% set filter_agg = qcol ~ ' IS NOT NULL' %}
    {% else %}
        {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ (incomingUserValue | string) ~ ')' %}
    {% endif %}

    {% if replaceWithType == 'average' %}
        {% set rep_expr %}AVG(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END){% endset %}
    {% elif replaceWithType == 'median' %}
        {% set rep_expr %}APPROX_QUANTILES(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END, 100)[OFFSET(50)]{% endset %}
    {% elif replaceWithType == 'mode' %}
        {% set rep_expr %}(
            SELECT {{ qcol }}
            FROM base
            WHERE {{ filter_agg }}
            GROUP BY {{ qcol }}
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ){% endset %}
    {% else %}
        {% set rep_expr = replaceWithUserValue | string %}
    {% endif %}

    , stat_{{ loop.index0 }} AS (
        {% if replaceWithType == 'user' %}
            SELECT {{ rep_expr }} AS rep_val
        {% elif replaceWithType == 'mode' %}
            SELECT {{ rep_expr }} AS rep_val
        {% else %}
            SELECT {{ rep_expr }} AS rep_val FROM base
        {% endif %}
    )
{% endfor %}

, stats AS (
    SELECT
    {% for col_name in columnNames %}
        (SELECT rep_val FROM stat_{{ loop.index0 }}) AS rep_{{ loop.index0 }}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
)

SELECT
{% for c in schema %}
    {% set col_name = c.name %}
    {% set key = col_name | upper %}
    {% set in_impute_list = false %}
    {% set impute_idx = -1 %}
    {% for impute_col in columnNames %}
        {% if (impute_col | upper) == key %}
            {% set in_impute_list = true %}
            {% set impute_idx = loop.index0 %}
        {% endif %}
    {% endfor %}

    {% if not in_impute_list %}
        {{ bt ~ col_name ~ bt }}
    {% else %}
        {% set qcol = bt ~ col_name ~ bt %}

        {% if replaceIncomingType == 'null_val' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
            {% set replace_expr = 'COALESCE(' ~ qcol ~ ', (SELECT rep_' ~ impute_idx ~ ' FROM stats))' %}
        {% else %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
            {% set replace_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN (SELECT rep_' ~ impute_idx ~ ' FROM stats) ELSE ' ~ qcol ~ ' END' %}
        {% endif %}

        {% if outputImputedAsSeparateField %}
            {{ qcol }},
            {{ replace_expr }} AS {{ bt ~ col_name ~ '_ImputedValue' ~ bt }}
            {% if includeImputedIndicator %},
            IF({{ is_imputed_cond }}, 1, 0) AS {{ bt ~ col_name ~ '_Indicator' ~ bt }}
            {% endif %}
        {% else %}
            {{ replace_expr }} AS {{ qcol }}
            {% if includeImputedIndicator %},
            IF({{ is_imputed_cond }}, 1, 0) AS {{ bt ~ col_name ~ '_Indicator' ~ bt }}
            {% endif %}
        {% endif %}
    {% endif %}
    {%- if not loop.last %},{% endif %}
{% endfor %}
FROM base
CROSS JOIN stats

{% endif %}
{% endmacro %}


{# ============================================================
   SNOWFLAKE  (snowflake__)
   - Quoting  : double-quotes via prophecy_basics.quote_identifier
   - Median   : MEDIAN() native aggregate
   - Mode     : ORDER BY COUNT(*) DESC LIMIT 1 subquery
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
    {% set qcol = prophecy_basics.quote_identifier(col_name) %}

    {% if replaceIncomingType == 'null_val' %}
        {% set filter_agg = qcol ~ ' IS NOT NULL' %}
    {% else %}
        {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ (incomingUserValue | string) ~ ')' %}
    {% endif %}

    {% if replaceWithType == 'average' %}
        {% set rep_expr %}AVG(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END){% endset %}
    {% elif replaceWithType == 'median' %}
        {% set rep_expr %}MEDIAN(CASE WHEN {{ filter_agg }} THEN {{ qcol }} END){% endset %}
    {% elif replaceWithType == 'mode' %}
        {# Snowflake has no MODE() — use ORDER BY COUNT(*) DESC LIMIT 1 #}
        {% set rep_expr %}(
            SELECT {{ qcol }}
            FROM base
            WHERE {{ filter_agg }}
            GROUP BY {{ qcol }}
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ){% endset %}
    {% else %}
        {% set rep_expr = replaceWithUserValue | string %}
    {% endif %}

    , stat_{{ loop.index0 }} AS (
        {% if replaceWithType == 'user' %}
            SELECT {{ rep_expr }} AS rep_val
        {% elif replaceWithType == 'mode' %}
            SELECT {{ rep_expr }} AS rep_val
        {% else %}
            SELECT {{ rep_expr }} AS rep_val FROM base
        {% endif %}
    )
{% endfor %}

, stats AS (
    SELECT
    {% for col_name in columnNames %}
        (SELECT rep_val FROM stat_{{ loop.index0 }}) AS rep_{{ loop.index0 }}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
)

SELECT
{% for c in schema %}
    {% set col_name = c.name %}
    {% set key = col_name | upper %}
    {% set in_impute_list = false %}
    {% set impute_idx = -1 %}
    {% for impute_col in columnNames %}
        {% if (impute_col | upper) == key %}
            {% set in_impute_list = true %}
            {% set impute_idx = loop.index0 %}
        {% endif %}
    {% endfor %}

    {% if not in_impute_list %}
        {{ prophecy_basics.quote_identifier(col_name) }}
    {% else %}
        {% set qcol = prophecy_basics.quote_identifier(col_name) %}

        {% if replaceIncomingType == 'null_val' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
            {% set replace_expr = 'COALESCE(' ~ qcol ~ ', (SELECT rep_' ~ impute_idx ~ ' FROM stats))' %}
        {% else %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
            {% set replace_expr = 'CASE WHEN ' ~ is_imputed_cond ~ ' THEN (SELECT rep_' ~ impute_idx ~ ' FROM stats) ELSE ' ~ qcol ~ ' END' %}
        {% endif %}

        {% if outputImputedAsSeparateField %}
            {{ qcol }},
            {{ replace_expr }} AS {{ prophecy_basics.quote_identifier(col_name ~ '_ImputedValue') }}
            {% if includeImputedIndicator %},
            CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}
            {% endif %}
        {% else %}
            {{ replace_expr }} AS {{ qcol }}
            {% if includeImputedIndicator %},
            CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}
            {% endif %}
        {% endif %}
    {% endif %}
    {%- if not loop.last %},{% endif %}
{% endfor %}
FROM base
CROSS JOIN stats

{% endif %}
{% endmacro %}