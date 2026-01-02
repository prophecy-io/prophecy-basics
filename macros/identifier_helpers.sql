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
    {% if text is none or text == '' %}
        {{ return(text) }}
    {% endif %}
    {% if plain_col is none or plain_col == '' %}
        {{ return(text) }}
    {% endif %}
    {% set word_chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' %}
    {% set suffixes = [' ', '(', '.', ')', ',', '+', '-', '*', '/', '%', '=', '<', '>', '|', '&'] %}
    {% set col_len = plain_col | length %}
    {% set text_len = text | length %}
    
    {# Step 1: Find all positions where column name appears in original text #}
    {# Use namespace to maintain state across loop iterations (Jinja2 limitation) #}
    {% set ns = namespace(positions=[], search_pos=0) %}
    {% for _ in range(0, text_len) %}
        {% if ns.search_pos >= text_len %}
            {% break %}
        {% endif %}
        {% set remaining = text[ns.search_pos:] %}
        {% set found_pos = remaining.find(plain_col) %}
        {% if found_pos >= 0 %}
            {% set actual_pos = ns.search_pos + found_pos %}
            {% do ns.positions.append(actual_pos) %}
            {# Advance past the match to avoid finding it again #}
            {% set ns.search_pos = actual_pos + col_len %}
        {% else %}
            {% break %}
        {% endif %}
    {% endfor %}
    {% set positions = ns.positions %}
    
    {# Step 2: Process each position and build result #}
    {% set result_parts = [] %}
    {% set ns2 = namespace(last_pos=0) %}
    {% for match_pos in positions %}
        {% set before_pos = match_pos - 1 %}
        {% set after_pos = match_pos + col_len %}
        {% set before_char = '' if before_pos < 0 else text[before_pos] %}
        {% set after_char = '' if after_pos >= text_len else text[after_pos] %}
        {# If before_char is '.', it's a qualified column (e.g., payload.YMD) - don't replace #}
        {% set valid_before = match_pos == 0 or (before_char != '.' and before_char not in word_chars) %}
        {% set valid_after = after_pos >= text_len or after_char in suffixes or after_char not in word_chars %}
        
        {# Add text before this match #}
        {% do result_parts.append(text[ns2.last_pos:match_pos]) %}
        
        {# Replace if valid, otherwise keep original #}
        {% if valid_before and valid_after %}
            {% do result_parts.append(replacement) %}
        {% else %}
            {% do result_parts.append(plain_col) %}
        {% endif %}
        
        {% set ns2.last_pos = after_pos %}
    {% endfor %}
    
    {# Add remaining text after last match #}
    {% do result_parts.append(text[ns2.last_pos:]) %}
    
    {% set result = result_parts | join('') %}
    {% if result == '' %}
        {% set result = text %}
    {% endif %}
    {{ return(result) }}
{% endmacro %}

{# Helper: Replace all variants of a column name (quoted and unquoted) with replacement, preserving payload references #}
{% macro replace_column_in_expression(text, unquoted_col, replacement, preserve_payload=true) %}
    {% set q_by_adapter = prophecy_basics.quote_identifier(unquoted_col) %}
    {% set backtick_col = "`" ~ unquoted_col ~ "`" %}
    {% set doubleq_col = '"' ~ unquoted_col ~ '"' %}
    {% set singleq_col = "'" ~ unquoted_col ~ "'" %}
    {% set plain_col = unquoted_col %}
    
    {# Replace plain column name first (on original text) to avoid matching in replacements #}
    {% set result = prophecy_basics.replace_column_safe(text, plain_col, replacement) %}
    {# Then replace quoted variants (these shouldn't conflict since they're already quoted) #}
    {% set result = result | replace(q_by_adapter, replacement) %}
    {% set result = result | replace(backtick_col, replacement) %}
    {% set result = result | replace(doubleq_col, replacement) %}
    {% set result = result | replace(singleq_col, replacement) %}
    {# Restore payload references if requested #}
    {% if preserve_payload %}
        {% set result = result | replace('payload.' ~ replacement, 'payload.' ~ plain_col) %}
    {% endif %}
    {{ return(result) }}
{% endmacro %}
