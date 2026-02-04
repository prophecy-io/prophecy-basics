{% macro SchemaInfo(relation_name, schema) -%}
    {{ return(adapter.dispatch('SchemaInfo', 'prophecy_basics')(relation_name, schema)) }}
{%- endmacro %}

{%- macro default__SchemaInfo(relation_name, schema) -%}
    {# schema is expected to be a JSON array of objects with keys: name, dataType, size, description, source, scale #}
    {%- set parsed_schema = [] -%}

    {%- if schema is not none and schema != '' and schema != '[]' and schema != "''" -%}
        {%- if schema is string -%}
            {%- set parsed_schema = fromjson(schema) -%}
        {%- else -%}
            {%- set parsed_schema = schema -%}
        {%- endif -%}
    {%- endif -%}

    {%- if parsed_schema | length == 0 -%}
        select *
        from (
            select cast(null as string) as Name,
                   cast(null as string) as Type,
                   cast(null as int) as Size,
                   cast(null as string) as Description,
                   cast(null as string) as Source,
                   cast(null as int) as Scale
        ) empty_schema
        where 1 = 0
    {%- else -%}
        select *
        from values
        {%- for col in parsed_schema %}
            (
                '{{ prophecy_basics.escape_sql_string(col["name"], true) if col.get("name") is not none else '' }}',
                '{{ col.get("dataType", '') }}',
                {{ col.get("size", 4) if col.get("size") is not none else 4 }},
                '{{ prophecy_basics.escape_sql_string(col.get("description", ''), true) }}',
                '{{ prophecy_basics.escape_sql_string(col.get("source", ''), true) }}',
                {%- if col.get("scale") is not none -%}{{ col.get("scale") }}{%- else -%}cast(null as int){%- endif -%}
            ){% if not loop.last %},{% endif %}
        {%- endfor %}
        as schema_info(Name, Type, Size, Description, Source, Scale)
    {%- endif %}
{%- endmacro %}

{%- macro bigquery__SchemaInfo(relation_name, schema) -%}
    {%- set parsed_schema = [] -%}

    {%- if schema is not none and schema != '' and schema != '[]' and schema != "''" -%}
        {%- if schema is string -%}
            {%- set parsed_schema = fromjson(schema) -%}
        {%- else -%}
            {%- set parsed_schema = schema -%}
        {%- endif -%}
    {%- endif -%}

    {%- if parsed_schema | length == 0 -%}
        select *
        from (
            select cast(null as string) as Name,
                   cast(null as string) as Type,
                   cast(null as int64) as Size,
                   cast(null as string) as Description,
                   cast(null as string) as Source,
                   cast(null as int64) as Scale
        ) empty_schema
        where 1 = 0
    {%- else -%}
        select *
        from unnest([
            {%- for col in parsed_schema %}
                struct(
                    '{{ prophecy_basics.escape_sql_string(col["name"], true) if col.get("name") is not none else '' }}' as Name,
                    '{{ col.get("dataType", '') }}' as Type,
                    {{ col.get("size", 4) if col.get("size") is not none else 4 }} as Size,
                    '{{ prophecy_basics.escape_sql_string(col.get("description", ''), true) }}' as Description,
                    '{{ prophecy_basics.escape_sql_string(col.get("source", ''), true) }}' as Source,
                    {%- if col.get("scale") is not none -%}{{ col.get("scale") }}{%- else -%}cast(null as int64){%- endif -%} as Scale
                ){% if not loop.last %}, {% endif %}
            {%- endfor %}
        ]) as schema_info
    {%- endif %}
{%- endmacro %}
