{# Helper macros for proper identifier handling across different database engines #}

{% macro quote_identifier(identifier) %}
    {% if identifier is string %}
        {% set clean_identifier = identifier | trim %}
        {% if ' ' in clean_identifier or clean_identifier.startswith('`') == false %}
            {% set bt = "`" %}
            {{ bt ~ clean_identifier ~ bt }}
        {% else %}
            {{ clean_identifier }}
        {% endif %}
    {% else %}
        {{ identifier }}
    {% endif %}
{% endmacro %}

{% macro unquote_identifier(identifier) %}
    {% if identifier is string %}
        {% set clean_identifier = identifier | trim %}
        {% if clean_identifier.startswith('`') and clean_identifier.endswith('`') %}
            {{ clean_identifier[1:-1] }}
        {% else %}
            {{ clean_identifier }}
        {% endif %}
    {% else %}
        {{ identifier }}
    {% endif %}
{% endmacro %}

{% macro safe_identifier(identifier) %}
    {# Returns a safe identifier that can be used in SQL queries #}
    {% if identifier is string %}
        {% set clean_identifier = identifier | trim %}
        {% if ' ' in clean_identifier or clean_identifier.startswith('`') == false %}
            {% set bt = "`" %}
            {{ bt ~ clean_identifier ~ bt }}
        {% else %}
            {{ clean_identifier }}
        {% endif %}
    {% else %}
        {{ identifier }}
    {% endif %}
{% endmacro %}
