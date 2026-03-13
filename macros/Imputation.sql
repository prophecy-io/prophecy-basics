{% macro Imputation(relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField) -%}
    {{ return(adapter.dispatch('Imputation', 'prophecy_basics')(relation_name,
        schema,
        columnNames,
        replaceIncomingType,
        incomingUserValue,
        replaceWithType,
        replaceWithUserValue,
        includeImputedIndicator,
        outputImputedAsSeparateField)) }}
{% endmacro %}

{% macro default__Imputation(relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField) %}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set bt = "`" %}

    {% if columnNames | length == 0 %}
        SELECT * FROM {{ relation_list | join(', ') }}
    {% else %}

    {# Build filter for "values to include in aggregate" (exclude incoming value to replace) #}
    {# replaceIncomingType: 'null_val' = replace NULLs, so aggregate over non-nulls. 'user' = replace user value, so exclude that value. #}
    {% set numeric_types = ["bigint","decimal","double","float","integer","smallint","tinyint","long","short"] %}
    {% set col_type_map = {} %}
    {% for c in schema %}
        {% do col_type_map.update({ c.name: (c.dataType | lower) }) %}
    {% endfor %}

    WITH base AS (
        SELECT * FROM {{ relation_list | join(', ') }}
    )
    {% for col_name in columnNames %}
        {% set dtype = col_type_map.get(col_name) %}
        {% set qcol = bt ~ col_name ~ bt %}
        {% if replaceIncomingType == 'null_val' %}
            {% set filter_agg = qcol ~ ' IS NOT NULL' %}
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
        {% else %}
            {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ (incomingUserValue | string) ~ ')' %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
        {% endif %}

        {% if replaceWithType == 'average' %}
            {% set rep_expr = 'AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% elif replaceWithType == 'median' %}
            {% set rep_expr = 'PERCENTILE_APPROX(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END, 0.5)' %}
        {% elif replaceWithType == 'mode' %}
            {% set rep_expr = '(SELECT _m.' ~ qcol ~ ' FROM (SELECT ' ~ qcol ~ ', COUNT(*) AS _cnt FROM base WHERE ' ~ filter_agg ~ ' GROUP BY ' ~ qcol ~ ' ORDER BY _cnt DESC LIMIT 1) _m)' %}
        {% else %}
            {% set rep_expr = (replaceWithUserValue | string) %}
        {% endif %}

        {% if loop.index0 > 0 %},{% endif %}
        {% if replaceWithType == 'user' %}
            stat_{{ loop.index0 }} AS (SELECT {{ rep_expr }} AS rep_val)
        {% else %}
            stat_{{ loop.index0 }} AS (SELECT {{ rep_expr }} AS rep_val FROM base)
        {% endif %}
    {% endfor %}
    , stats AS (
        SELECT
        {% for col_name in columnNames %}
            (SELECT rep_val FROM stat_{{ loop.index0 }}) AS rep_{{ loop.index0 }}{% if not loop.last %},{% endif %}
        {% endfor %}
        FROM base LIMIT 1
    )
    SELECT
    {% for c in schema %}
        {% set col_name = c.name %}
        {% set key = col_name | replace('`','') | replace('"','') | upper %}
        {% set in_impute_list = false %}
        {% set impute_idx = -1 %}
        {% for impute_col in columnNames %}
            {% if (impute_col | replace('`','') | replace('"','') | upper) == key %}
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
            {% else %}
                {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
            {% endif %}
            {% if outputImputedAsSeparateField %}
                {{ qcol }},
                COALESCE(CASE WHEN {{ is_imputed_cond }} THEN (SELECT rep_{{ impute_idx }} FROM stats) END, {{ qcol }}) AS {{ prophecy_basics.quote_identifier(col_name ~ '_ImputedValue') }}
                {% if includeImputedIndicator %}, CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}{% endif %}
            {% else %}
                COALESCE(CASE WHEN {{ is_imputed_cond }} THEN (SELECT rep_{{ impute_idx }} FROM stats) END, {{ qcol }}) AS {{ qcol }}
                {% if includeImputedIndicator %}, CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}{% endif %}
            {% endif %}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM base
    CROSS JOIN stats
    {% endif %}
{% endmacro %}


{% macro duckdb__Imputation(relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField) %}

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
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
        {% else %}
            {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' <> ' ~ (incomingUserValue | string) ~ ')' %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
        {% endif %}

        {% if replaceWithType == 'average' %}
            {% set rep_expr = 'AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% elif replaceWithType == 'median' %}
            {% set rep_expr = 'MEDIAN(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% elif replaceWithType == 'mode' %}
            {% set rep_expr = 'MODE(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% else %}
            {% set rep_expr = (replaceWithUserValue | string) %}
        {% endif %}

        {% if loop.index0 > 0 %},{% endif %}
        {% if replaceWithType == 'user' %}
            stat_{{ loop.index0 }} AS (SELECT {{ rep_expr }} AS rep_val)
        {% else %}
            stat_{{ loop.index0 }} AS (SELECT {{ rep_expr }} AS rep_val FROM base)
        {% endif %}
    {% endfor %}
    , stats AS (
        SELECT
        {% for col_name in columnNames %}
            (SELECT rep_val FROM stat_{{ loop.index0 }}) AS rep_{{ loop.index0 }}{% if not loop.last %},{% endif %}
        {% endfor %}
        FROM base LIMIT 1
    )
    SELECT
    {% for c in schema %}
        {% set col_name = c.name %}
        {% set key = col_name | replace('`','') | replace('"','') | upper %}
        {% set in_impute_list = false %}
        {% set impute_idx = -1 %}
        {% for impute_col in columnNames %}
            {% if (impute_col | replace('`','') | replace('"','') | upper) == key %}
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
            {% else %}
                {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
            {% endif %}
            {% if outputImputedAsSeparateField %}
                {{ qcol }},
                COALESCE(CASE WHEN {{ is_imputed_cond }} THEN (SELECT rep_{{ impute_idx }} FROM stats) END, {{ qcol }}) AS {{ prophecy_basics.quote_identifier(col_name ~ '_ImputedValue') }}
                {% if includeImputedIndicator %}, CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}{% endif %}
            {% else %}
                COALESCE(CASE WHEN {{ is_imputed_cond }} THEN (SELECT rep_{{ impute_idx }} FROM stats) END, {{ qcol }}) AS {{ qcol }}
                {% if includeImputedIndicator %}, CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}{% endif %}
            {% endif %}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM base
    CROSS JOIN stats
    {% endif %}
{% endmacro %}


{% macro bigquery__Imputation(relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField) %}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set bt = "`" %}

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
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
        {% else %}
            {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ (incomingUserValue | string) ~ ')' %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
        {% endif %}

        {% if replaceWithType == 'average' %}
            {% set rep_expr = 'AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% elif replaceWithType == 'median' %}
            {% set rep_expr = 'APPROX_QUANTILES(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END, 100)[OFFSET(50)]' %}
        {% elif replaceWithType == 'mode' %}
            {% set rep_expr = '(SELECT col FROM (SELECT ' ~ qcol ~ ' AS col, COUNT(*) AS c FROM base WHERE ' ~ filter_agg ~ ' GROUP BY col ORDER BY c DESC LIMIT 1))' %}
        {% else %}
            {% set rep_expr = (replaceWithUserValue | string) %}
        {% endif %}

        {% if loop.index0 > 0 %},{% endif %}
        {% if replaceWithType == 'user' %}
            stat_{{ loop.index0 }} AS (SELECT {{ rep_expr }} AS rep_val)
        {% else %}
            stat_{{ loop.index0 }} AS (SELECT {{ rep_expr }} AS rep_val FROM base)
        {% endif %}
    {% endfor %}
    , stats AS (
        SELECT
        {% for col_name in columnNames %}
            (SELECT rep_val FROM stat_{{ loop.index0 }}) AS rep_{{ loop.index0 }}{% if not loop.last %},{% endif %}
        {% endfor %}
        FROM base LIMIT 1
    )
    SELECT
    {% for c in schema %}
        {% set col_name = c.name %}
        {% set key = col_name | replace('`','') | replace('"','') | upper %}
        {% set in_impute_list = false %}
        {% set impute_idx = -1 %}
        {% for impute_col in columnNames %}
            {% if (impute_col | replace('`','') | replace('"','') | upper) == key %}
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
            {% else %}
                {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
            {% endif %}
            {% if outputImputedAsSeparateField %}
                {{ qcol }},
                COALESCE(CASE WHEN {{ is_imputed_cond }} THEN (SELECT rep_{{ impute_idx }} FROM stats) END, {{ qcol }}) AS {{ bt ~ col_name ~ '_ImputedValue' ~ bt }}
                {% if includeImputedIndicator %}, IF({{ is_imputed_cond }}, 1, 0) AS {{ bt ~ col_name ~ '_Indicator' ~ bt }}{% endif %}
            {% else %}
                COALESCE(CASE WHEN {{ is_imputed_cond }} THEN (SELECT rep_{{ impute_idx }} FROM stats) END, {{ qcol }}) AS {{ qcol }}
                {% if includeImputedIndicator %}, IF({{ is_imputed_cond }}, 1, 0) AS {{ bt ~ col_name ~ '_Indicator' ~ bt }}{% endif %}
            {% endif %}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM base
    CROSS JOIN stats
    {% endif %}
{% endmacro %}


{% macro snowflake__Imputation(relation_name,
    schema,
    columnNames,
    replaceIncomingType,
    incomingUserValue,
    replaceWithType,
    replaceWithUserValue,
    includeImputedIndicator,
    outputImputedAsSeparateField) %}

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
            {% set is_imputed_cond = qcol ~ ' IS NULL' %}
        {% else %}
            {% set filter_agg = '(' ~ qcol ~ ' IS NULL OR ' ~ qcol ~ ' != ' ~ (incomingUserValue | string) ~ ')' %}
            {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
        {% endif %}

        {% if replaceWithType == 'average' %}
            {% set rep_expr = 'AVG(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% elif replaceWithType == 'median' %}
            {% set rep_expr = 'MEDIAN(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% elif replaceWithType == 'mode' %}
            {% set rep_expr = 'MODE(CASE WHEN ' ~ filter_agg ~ ' THEN ' ~ qcol ~ ' END)' %}
        {% else %}
            {% set rep_expr = (replaceWithUserValue | string) %}
        {% endif %}

        {% if loop.index0 > 0 %},{% endif %}
        {% if replaceWithType == 'user' %}
            stat_{{ loop.index0 }} AS (SELECT {{ rep_expr }} AS rep_val)
        {% else %}
            stat_{{ loop.index0 }} AS (SELECT {{ rep_expr }} AS rep_val FROM base)
        {% endif %}
    {% endfor %}
    , stats AS (
        SELECT
        {% for col_name in columnNames %}
            (SELECT rep_val FROM stat_{{ loop.index0 }}) AS rep_{{ loop.index0 }}{% if not loop.last %},{% endif %}
        {% endfor %}
        FROM base LIMIT 1
    )
    SELECT
    {% for c in schema %}
        {% set col_name = c.name %}
        {% set key = col_name | replace('`','') | replace('"','') | upper %}
        {% set in_impute_list = false %}
        {% set impute_idx = -1 %}
        {% for impute_col in columnNames %}
            {% if (impute_col | replace('`','') | replace('"','') | upper) == key %}
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
            {% else %}
                {% set is_imputed_cond = qcol ~ ' = ' ~ (incomingUserValue | string) %}
            {% endif %}
            {% if outputImputedAsSeparateField %}
                {{ qcol }},
                COALESCE(CASE WHEN {{ is_imputed_cond }} THEN (SELECT rep_{{ impute_idx }} FROM stats) END, {{ qcol }}) AS {{ prophecy_basics.quote_identifier(col_name ~ '_ImputedValue') }}
                {% if includeImputedIndicator %}, CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}{% endif %}
            {% else %}
                COALESCE(CASE WHEN {{ is_imputed_cond }} THEN (SELECT rep_{{ impute_idx }} FROM stats) END, {{ qcol }}) AS {{ qcol }}
                {% if includeImputedIndicator %}, CASE WHEN {{ is_imputed_cond }} THEN 1 ELSE 0 END AS {{ prophecy_basics.quote_identifier(col_name ~ '_Indicator') }}{% endif %}
            {% endif %}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM base
    CROSS JOIN stats
    {% endif %}
{% endmacro %}
