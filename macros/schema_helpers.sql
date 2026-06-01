{# Coerce macro args that Prophecy may pass as JSON strings (e.g. schema, columnNames).
   If left as strings, Jinja {% for x in value %} iterates characters and breaks SQL generation. #}
{% macro parse_macro_json_array(value) %}
    {% if value is undefined or value is none %}
        {{ return([]) }}
    {% elif value is string %}
        {% set _t = value | trim %}
        {% if _t == '' or _t == '[]' or _t == "''" %}
            {{ return([]) }}
        {% else %}
            {{ return(fromjson(_t)) }}
        {% endif %}
    {% else %}
        {{ return(value) }}
    {% endif %}
{% endmacro %}

{# Imputation (and similar) macros: normalize both list args in one place. #}
{% macro normalize_imputation_args(schema, columnNames) %}
    {% set _s = prophecy_basics.parse_macro_json_array(schema) %}
    {% set _c = prophecy_basics.parse_macro_json_array(columnNames) %}
    {{ return([_s, _c]) }}
{% endmacro %}

{% macro column_exists_in_schema(schema, column_name) %}
    {# Check if a column exists in the provided schema #}
    {# Schema is a JSON array of objects with 'name' and 'dataType' keys #}
    {# Returns true if column exists, false otherwise #}
    {% if schema is none or schema == '' or schema == '[]' or schema == "''" or schema is undefined %}
        {{ return(false) }}
    {% endif %}

    {% set schema_parsed = prophecy_basics.parse_macro_json_array(schema) %}

    {# Check if parsing was successful and result is iterable #}
    {% if schema_parsed is none %}
        {{ return(false) }}
    {% endif %}

    {% if schema_parsed is not iterable or schema_parsed is string or schema_parsed is number %}
        {{ return(false) }}
    {% endif %}

    {% set unquoted_col = prophecy_basics.unquote_identifier(column_name) | trim %}

    {# Iterate through schema objects and check if column name matches (using adapter-specific case sensitivity) #}
    {% for c in schema_parsed %}
        {# Each c should be an object with 'name' and 'dataType' keys #}
        {% set schema_col_name = c.name | trim %}
        {% if prophecy_basics.column_names_match(schema_col_name, unquoted_col) %}
            {{ return(true) }}
        {% endif %}
    {% endfor %}

    {{ return(false) }}
{% endmacro %}
