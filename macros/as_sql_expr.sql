{% macro as_sql_expr(val) %}
    {%- if val is none -%}
        NULL
    {%- else -%}
        {%- set v = val | trim -%}

        {# Remove outer single quotes #}
        {%- if v.startswith("'") and v.endswith("'") -%}
            {%- set v = v[1:-1] -%}
        {%- endif -%}

        {# Remove outer double quotes #}
        {%- if v.startswith('"') and v.endswith('"') -%}
            {%- set v = v[1:-1] -%}
        {%- endif -%}

        {# Unescape common patterns #}
        {%- set v = v
            | replace("\\'", "'")
            | replace('\\"', '"')
            | replace("''", "'")
        -%}

        {{ v }}
    {%- endif -%}
{% endmacro %}
