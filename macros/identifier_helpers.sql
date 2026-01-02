{# Helper macros for proper identifier handling across different database engines #}

{% macro quote_identifier(identifier) %}
    {{ return(adapter.dispatch('quote_identifier', 'prophecy_basics')(identifier)) }}
{% endmacro %}

{% macro default__quote_identifier(identifier) %}
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

{% macro bigquery__quote_identifier(identifier) %}
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

{% macro duckdb__quote_identifier(identifier, quote_char='"') %}
    {%- if identifier is string -%}
        {# Single identifier - return quoted string #}
        {{ quote_char }}{{ identifier }}{{ quote_char }}
    {%- elif identifier is iterable -%}
        {# List of identifiers - return list of quoted strings #}
        {%- set quoted_identifiers = [] -%}
        {%- for id in identifier -%}
            {%- do quoted_identifiers.append(quote_char ~ id ~ quote_char) -%}
        {%- endfor -%}
        {{ quoted_identifiers }}
    {%- else -%}
        {# Fallback: treat as string #}
        {{ quote_char }}{{ identifier }}{{ quote_char }}
    {%- endif -%}
{% endmacro %}

{% macro unquote_identifier(identifier) %}
    {{ return(adapter.dispatch('unquote_identifier', 'prophecy_basics')(identifier)) }}
{% endmacro %}

{% macro default__unquote_identifier(identifier) %}
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

{% macro duckdb__unquote_identifier(identifier) %}
    {%- if identifier is string -%}
        {# Single identifier - unquote if quoted #}
        {%- set clean_identifier = identifier | trim -%}
        {%- if clean_identifier.startswith('"') and clean_identifier.endswith('"') -%}
            {{ clean_identifier[1:-1] }}
        {%- else -%}
            {{ clean_identifier }}
        {%- endif -%}
    {%- elif identifier is iterable -%}
        {# List of identifiers - return list of unquoted strings #}
        {%- set unquoted_identifiers = [] -%}
        {%- for id in identifier -%}
            {%- set clean_id = id | trim -%}
            {%- if clean_id.startswith('"') and clean_id.endswith('"') -%}
                {%- do unquoted_identifiers.append(clean_id[1:-1]) -%}
            {%- else -%}
                {%- do unquoted_identifiers.append(clean_id) -%}
            {%- endif -%}
        {%- endfor -%}
        {{ unquoted_identifiers }}
    {%- else -%}
        {# Fallback: treat as string #}
        {{ identifier }}
    {%- endif -%}
{% endmacro %}

{% macro safe_identifier(identifier) %}
    {{ return(adapter.dispatch('safe_identifier', 'prophecy_basics')(identifier)) }}
{% endmacro %}

{% macro default__safe_identifier(identifier) %}
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

{% macro duckdb__safe_identifier(identifier, quote_char='"') %}
    {# Returns a safe identifier that can be used in SQL queries #}
    {%- if identifier is string -%}
        {# Single identifier - return quoted string #}
        {{ quote_char }}{{ identifier }}{{ quote_char }}
    {%- elif identifier is iterable -%}
        {# List of identifiers - return list of quoted strings #}
        {%- set quoted_identifiers = [] -%}
        {%- for id in identifier -%}
            {%- do quoted_identifiers.append(quote_char ~ id ~ quote_char) -%}
        {%- endfor -%}
        {{ quoted_identifiers }}
    {%- else -%}
        {# Fallback: treat as string #}
        {{ quote_char }}{{ identifier }}{{ quote_char }}
    {%- endif -%}
{% endmacro %}

{# Helper: Replace column name only when its a complete identifier (not a substring) #}
{% macro replace_column_safe(text, plain_col, replacement) %}
    {% set word_chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
    {% set suffixes = [' ', '(', '.', ')', ',', '+', '-', '*', '/', '%', '=', '<', '>', '|', '&'] %}
    {% set col_len = plain_col | length %}
    {% set text_len = text | length %}
    {% set result = '' %}
    {% set i = 0 %}
    {% for _ in range(0, text_len) %}
        {% if i >= text_len %}
            {% break %}
        {% endif %}
        {% set remaining = text[i:] %}
        {% set pos = remaining.find(plain_col) %}
        {% if pos >= 0 %}
            {% set actual_pos = i + pos %}
            {% set before_pos = actual_pos - 1 %}
            {% set after_pos = actual_pos + col_len %}
            {% set before_char = '' if before_pos < 0 else text[before_pos] %}
            {% set after_char = '' if after_pos >= text_len else text[after_pos] %}
            {% set valid_before = before_pos < 0 or before_char not in word_chars %}
            {% set valid_after = after_pos >= text_len or after_char in suffixes or after_char not in word_chars %}
            {% if valid_before and valid_after %}
                {% set result = result ~ text[i:actual_pos] ~ replacement %}
                {% set i = after_pos %}
            {% else %}
                {% set result = result ~ text[i:actual_pos + 1] %}
                {% set i = actual_pos + 1 %}
            {% endif %}
        {% else %}
            {% set result = result ~ text[i:] %}
            {% set i = text_len %}
        {% endif %}
    {% endfor %}
    {{ return(result) }}
{% endmacro %}

{# Helper: Replace all variants of a column name (quoted and unquoted) with replacement, preserving payload references #}
{% macro replace_column_in_expression(text, unquoted_col, replacement, preserve_payload=true) %}
    {% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set backtick_col = "`" ~ unquoted_col ~ "`" %}
    {% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
    {% set singleq_col = "'" ~ unquoted_col ~ "'" %}
    {% set plain_col = unquoted_col %}
    
    {% set result = text %}
    {# Replace quoted variants first #}
    {% set result = result | replace(q_by_adapter, replacement) %}
    {% set result = result | replace(backtick_col, replacement) %}
    {% set result = result | replace(doubleq_col, replacement) %}
    {% set result = result | replace(singleq_col, replacement) %}
    {# Replace plain column name (word-boundary aware) #}
    {% set result = prophecy_basics.replace_column_safe(result, plain_col, replacement) %}
    {# Restore payload references if requested #}
    {% if preserve_payload %}
        {% set result = result | replace('payload.' ~ replacement, 'payload.' ~ plain_col) %}
    {% endif %}
    {{ return(result) }}
{% endmacro %}
