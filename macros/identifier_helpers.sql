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

{% macro quote_column_list(column_list) %}
    {# Takes a comma-separated string of column names and returns a quoted, comma-separated string #}
    {{ return(adapter.dispatch('quote_column_list', 'prophecy_basics')(column_list)) }}
{% endmacro %}

{% macro default__quote_column_list(column_list) %}
    {%- if column_list == "" or column_list is none -%}
        {{ return("") }}
    {%- else -%}
        {%- set quoted_columns = [] -%}
        {%- set column_list_split = column_list.split(',') | map('trim') | list -%}
        {%- for col in column_list_split -%}
            {%- if col != "" -%}
                {%- do quoted_columns.append(prophecy_basics.quote_identifier(col)) -%}
            {%- endif -%}
        {%- endfor -%}
        {{ return(quoted_columns | join(', ')) }}
    {%- endif -%}
{% endmacro %}

{% macro duckdb__quote_column_list(column_list) %}
    {%- if column_list == "" or column_list is none -%}
        {{ return("") }}
    {%- else -%}
        {%- set quoted_columns = [] -%}
        {%- set column_list_split = column_list.split(',') | map('trim') | list -%}
        {%- for col in column_list_split -%}
            {%- if col != "" -%}
                {%- do quoted_columns.append(prophecy_basics.quote_identifier(col)) -%}
            {%- endif -%}
        {%- endfor -%}
        {{ return(quoted_columns | join(', ')) }}
    {%- endif -%}
{% endmacro %}

{% macro bigquery__quote_column_list(column_list) %}
    {# BigQuery uses backticks for identifiers, same as default #}
    {%- if column_list == "" or column_list is none -%}
        {{ return("") }}
    {%- else -%}
        {%- set quoted_columns = [] -%}
        {%- set column_list_split = column_list.split(',') | map('trim') | list -%}
        {%- for col in column_list_split -%}
            {%- if col != "" -%}
                {%- do quoted_columns.append(prophecy_basics.quote_identifier(col)) -%}
            {%- endif -%}
        {%- endfor -%}
        {{ return(quoted_columns | join(', ')) }}
    {%- endif -%}
{% endmacro %}

{% macro cast_timestamp_if_needed(expr) %}
    {% if expr is none or expr == '' %}
        {{ return(expr) }}
    {% endif %}

    {% set expr_trimmed = expr | trim %}
    {% set is_timestamp_string = false %}
    {% set timestamp_str_to_cast = expr %}

    {# First check if it matches timestamp pattern (YYYY-MM-DD HH:MM:SS) #}
    {% set looks_like_timestamp = false %}
    {% set inner_timestamp_str = expr_trimmed %}
    {% set is_quoted = false %}

    {# Extract inner string if quoted #}
    {% if expr_trimmed.startswith("'") and expr_trimmed.endswith("'") %}
        {% set inner_timestamp_str = expr_trimmed[1:-1] | trim %}
        {% set is_quoted = true %}
    {% elif expr_trimmed.startswith('"') and expr_trimmed.endswith('"') %}
        {% set inner_timestamp_str = expr_trimmed[1:-1] | trim %}
        {% set is_quoted = true %}
    {% else %}
        {% set inner_timestamp_str = expr_trimmed | trim %}
    {% endif %}

    {# Check if inner string matches timestamp pattern (19 chars: YYYY-MM-DD HH:MM:SS) #}
    {% if inner_timestamp_str | length == 19 %}
        {% if inner_timestamp_str[4] == '-' and inner_timestamp_str[7] == '-' and inner_timestamp_str[10] == ' ' and inner_timestamp_str[13] == ':' and inner_timestamp_str[16] == ':' %}
            {# Verify it's actually digits and valid format #}
            {% set is_valid = true %}
            {% for i in [0,1,2,3,5,6,8,9,11,12,14,15,17,18] %}
                {% if inner_timestamp_str[i] not in '0123456789' %}
                    {% set is_valid = false %}
                    {% break %}
                {% endif %}
            {% endfor %}
            {% if is_valid %}
                {% set looks_like_timestamp = true %}
            {% endif %}
        {% endif %}
    {% endif %}

    {# Only cast if it looks like a timestamp AND doesn't contain expression operators #}
    {% if looks_like_timestamp %}
        {% set has_operators = '(' in expr_trimmed or ')' in expr_trimmed or '+' in expr_trimmed or '*' in expr_trimmed or '/' in expr_trimmed or 'payload.' in expr_trimmed %}
        {% if not expr_trimmed.startswith("'") and not expr_trimmed.startswith('"') %}
            {# For unquoted strings, check if there are extra spaces (other than the one at position 10) or extra operators #}
            {% if expr_trimmed.count(' ') > 1 or expr_trimmed.count('-') > 2 or expr_trimmed.count(':') > 2 %}
                {% set has_operators = true %}
            {% endif %}
        {% endif %}

        {% if not has_operators %}
            {% set is_timestamp_string = true %}
            {% if is_quoted %}
                {# Already quoted, use as-is #}
                {% set timestamp_str_to_cast = expr %}
            {% else %}
                {# Not quoted, add quotes #}
                {% set timestamp_str_to_cast = "'" ~ inner_timestamp_str ~ "'" %}
            {% endif %}
        {% endif %}
    {% endif %}

    {% if is_timestamp_string %}
        {{ return('CAST(' ~ timestamp_str_to_cast ~ ' AS TIMESTAMP)') }}
    {% else %}
        {# If not a timestamp, check if it's a date-only pattern #}
        {% set date_result = prophecy_basics.cast_date_if_needed(expr) %}
        {{ return(date_result) }}
    {% endif %}
{% endmacro %}
