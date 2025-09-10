{% macro DataCleansing(relation_name,
        schema,
        modifyCase,
        columnNames=[],
        replaceNullTextFields=False,
        replaceNullTextWith="NA",
        replaceNullForNumericFields=False,
        replaceNullNumericWith=0,
        trimWhiteSpace=False,
        removeTabsLineBreaksAndDuplicateWhitespace=False,
        allWhiteSpace=False,
        cleanLetters=False,
        cleanPunctuations=False,
        cleanNumbers=False,
        removeRowNullAllCols=False,
        replaceNullDateFields=False,
        replaceNullDateWith="1970-01-01",
        replaceNullTimeFields=False,
        replaceNullTimeWith="1970-01-01 00:00:00") -%}
    {{ return(adapter.dispatch('DataCleansing', 'prophecy_basics')(relation_name,
        schema,
        modifyCase,
        columnNames,
        replaceNullTextFields,
        replaceNullTextWith,
        replaceNullForNumericFields,
        replaceNullNumericWith,
        trimWhiteSpace,
        removeTabsLineBreaksAndDuplicateWhitespace,
        allWhiteSpace,
        cleanLetters,
        cleanPunctuations,
        cleanNumbers,
        removeRowNullAllCols,
        replaceNullDateFields,
        replaceNullDateWith,
        replaceNullTimeFields,
        replaceNullTimeWith)) }}
{% endmacro %}


{% macro databricks__DataCleansing(
        relation_name,
        schema,
        modifyCase,
        columnNames=[],
        replaceNullTextFields=False,
        replaceNullTextWith="NA",
        replaceNullForNumericFields=False,
        replaceNullNumericWith=0,
        trimWhiteSpace=False,
        removeTabsLineBreaksAndDuplicateWhitespace=False,
        allWhiteSpace=False,
        cleanLetters=False,
        cleanPunctuations=False,
        cleanNumbers=False,
        removeRowNullAllCols=False,
        replaceNullDateFields=False,
        replaceNullDateWith="1970-01-01",
        replaceNullTimeFields=False,
        replaceNullTimeWith="1970-01-01 00:00:00"
) %}

    {% set bt = "`" %}

    {# ───────────── 1) Row-level filter CTE ───────────── #}
    {% set cleansed_cte %}
        WITH cleansed_data AS (
            SELECT *
            FROM {{ relation_name }}
            {% if removeRowNullAllCols %}
                WHERE
                {%- set conds = [] -%}
                {%- for c in schema -%}
                    {%- do conds.append(bt ~ c.name ~ bt ~ ' IS NOT NULL') -%}
                {%- endfor -%}
                {{ conds | join(' OR ') }}
            {% endif %}
        )
    {% endset %}

    {# Early return when no columns requested #}
    {% if columnNames | length == 0 %}
        {{ return(cleansed_cte ~ '\nSELECT * FROM cleansed_data') }}
    {% endif %}

    {# ───────────── 2) Helpers ───────────── #}
    {% set numeric_types = [
        "bigint","decimal","double","float",
        "integer","smallint","tinyint"
    ] %}

    {# Map: column name -> lowercased data type #}
    {% set col_type_map = {} %}
    {% for c in schema %}
        {% do col_type_map.update({ c.name: c.dataType | lower }) %}
    {% endfor %}

    {# ───────────── 3) Build per-column override expressions ───────────── #}
    {% set override_map = {} %}

    {% for col_name in columnNames %}
        {% set dtype = col_type_map.get(col_name) %}
        {% set col_expr = bt ~ col_name ~ bt %}

        {# numeric null replacement #}
        {% if dtype in numeric_types and replaceNullForNumericFields %}
            {% set col_expr = "COALESCE(" ~ col_expr ~ ", " ~ (replaceNullNumericWith | string) ~ ")" %}
        {% endif %}

        {# string rules #}
        {% if dtype == "string" %}
            {% if replaceNullTextFields %}
                {% set col_expr = "COALESCE(" ~ col_expr ~ ", '" ~ replaceNullTextWith ~ "')" %}
            {% endif %}
            {% if trimWhiteSpace %}
                {% set col_expr = "LTRIM(RTRIM(" ~ col_expr ~ "))" %}
            {% endif %}
            {% if removeTabsLineBreaksAndDuplicateWhitespace %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', ' ')" %}
            {% endif %}
            {% if allWhiteSpace %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', '')" %}
            {% endif %}
            {% if cleanLetters %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[A-Za-z]+', '')" %}
            {% endif %}
            {% if cleanPunctuations %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[^a-zA-Z0-9\\\\s]', '')" %}
            {% endif %}
            {% if cleanNumbers %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\d+', '')" %}
            {% endif %}

            {% if modifyCase == 'makeLowercase' %}
                {% set col_expr = "LOWER(" ~ col_expr ~ ")" %}
            {% elif modifyCase == 'makeUppercase' %}
                {% set col_expr = "UPPER(" ~ col_expr ~ ")" %}
            {% elif modifyCase == 'makeTitlecase' %}
                {% set col_expr = "INITCAP(" ~ col_expr ~ ")" %}
            {% endif %}
        {% endif %}

        {# date / timestamp null replacement #}
        {% if dtype == 'date' and replaceNullDateFields %}
            {% set col_expr = "COALESCE(" ~ col_expr ~ ", DATE '" ~ replaceNullDateWith ~ "')" %}
        {% endif %}
        {% if dtype == 'timestamp' and replaceNullTimeFields %}
            {% set col_expr = "COALESCE(" ~ col_expr ~ ", TIMESTAMP '" ~ replaceNullTimeWith ~ "')" %}
        {% endif %}

        {# final cast back to original dtype; store override #}
        {% set final_expr = "CAST(" ~ col_expr ~ " AS " ~ dtype ~ ")" %}
        {% do override_map.update({ (col_name | upper): final_expr }) %}
    {% endfor %}

    {# ───────────── 4) Keep original order, apply overrides ───────────── #}
    {% set output_columns = [] %}
    {% for c in schema %}
        {% set key = c.name | replace('`','') | replace('"','') | upper %}
        {% if key in override_map %}
            {% do output_columns.append( override_map[key] ~ ' AS ' ~ bt ~ c.name ~ bt ) %}
        {% else %}
            {% do output_columns.append( bt ~ c.name ~ bt ) %}
        {% endif %}
    {% endfor %}

    {% set final_select %}
        SELECT {{ output_columns | join(', ') }} FROM cleansed_data
    {% endset %}

    {{ return(cleansed_cte ~ '\n' ~ final_select) }}

{% endmacro %}
