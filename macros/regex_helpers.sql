{# ============================================ #}
{# Regex Pattern Helpers                       #}
{# ============================================ #}

{# Helper macro to construct a regex pattern with only one capturing group #}
{# This is needed for BigQuery and DuckDB which have limitations on multiple capturing groups #}
{# Uses the full expression structure but only captures the target group #}
{% macro build_single_capture_pattern(full_pattern, parsed_columns, target_group_index, case_insensitive) %}
    {{ return(adapter.dispatch('build_single_capture_pattern', 'prophecy_basics')(full_pattern, parsed_columns, target_group_index, case_insensitive)) }}
{% endmacro %}

{# Default implementation: Use full pattern (for dialects that support multiple groups) #}
{% macro default__build_single_capture_pattern(full_pattern, parsed_columns, target_group_index, case_insensitive) %}
    {{ return(full_pattern) }}
{% endmacro %}

{# BigQuery-specific: Construct pattern with only target group capturing #}
{% macro bigquery__build_single_capture_pattern(full_pattern, parsed_columns, target_group_index, case_insensitive) %}
    {# Check for duplicate patterns in capture groups #}
    {%- set has_duplicates = false -%}
    {%- set pattern_counts = {} -%}
    {%- for config in parsed_columns -%}
        {%- set group_rgx = config.rgxExpression | default('') -%}
        {%- if group_rgx -%}
            {%- if pattern_counts[group_rgx] -%}
                {%- set has_duplicates = true -%}
            {%- else -%}
                {%- set _ = pattern_counts.update({group_rgx: 1}) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    
    {%- if has_duplicates -%}
        {# Step 1: Replace each group with enumerated placeholder #}
        {%- set temp_pattern = full_pattern -%}
        {%- set group_num = 0 -%}
        {%- for config in parsed_columns -%}
            {%- set group_num = group_num + 1 -%}
            {%- set group_rgx = config.rgxExpression | default('') -%}
            {%- if group_rgx -%}
                {%- set placeholder = '__GROUP_' ~ group_num ~ '__' -%}
                {# Always split by original pattern to find correct positions #}
                {%- set parts = full_pattern | split(group_rgx) -%}
                {%- set parts_count = parts | length -%}
                {# Count how many times this pattern appears before this group position #}
                {%- set occurrence_num = 0 -%}
                {%- for prev_num in range(1, group_num) -%}
                    {%- set prev_config = parsed_columns[prev_num - 1] -%}
                    {%- set prev_rgx = prev_config.rgxExpression | default('') -%}
                    {%- if prev_rgx == group_rgx -%}
                        {%- set occurrence_num = occurrence_num + 1 -%}
                    {%- endif -%}
                {%- endfor -%}
                {# The occurrence number for this group is (occurrence_num + 1) #}
                {%- set target_occurrence = occurrence_num + 1 -%}
                {# Create placeholder for this occurrence #}
                {# Reconstruct pattern by replacing the target occurrence with placeholder #}
                {%- if parts_count > 1 -%}
                    {%- set new_pattern = '' -%}
                    {%- for i in range(parts_count) -%}
                        {%- if i > 0 -%}
                            {%- if i == target_occurrence -%}
                                {# This is the target occurrence - replace with placeholder #}
                                {%- set new_pattern = new_pattern ~ placeholder -%}
                            {%- else -%}
                                {# Check if this position was already replaced with a placeholder from a previous group #}
                                {%- set use_original = true -%}
                                {%- for prev_num in range(1, group_num) -%}
                                    {%- set prev_config = parsed_columns[prev_num - 1] -%}
                                    {%- set prev_rgx = prev_config.rgxExpression | default('') -%}
                                    {%- if prev_rgx == group_rgx -%}
                                        {# Count occurrences of this pattern before prev_num #}
                                        {%- set prev_occurrence = 0 -%}
                                        {%- for p in range(1, prev_num) -%}
                                            {%- set p_config = parsed_columns[p - 1] -%}
                                            {%- set p_rgx = p_config.rgxExpression | default('') -%}
                                            {%- if p_rgx == group_rgx -%}
                                                {%- set prev_occurrence = prev_occurrence + 1 -%}
                                            {%- endif -%}
                                        {%- endfor -%}
                                        {%- set prev_occurrence = prev_occurrence + 1 -%}
                                        {%- if prev_occurrence == i -%}
                                            {# This position was already replaced by prev_num #}
                                            {%- set use_original = false -%}
                                            {%- set new_pattern = new_pattern ~ '__GROUP_' ~ prev_num ~ '__' -%}
                                            {%- break -%}
                                        {%- endif -%}
                                    {%- endif -%}
                                {%- endfor -%}
                                {%- if use_original -%}
                                    {# Use original pattern for this position #}
                                    {%- set new_pattern = new_pattern ~ group_rgx -%}
                                {%- endif -%}
                            {%- endif -%}
                        {%- endif -%}
                        {%- set new_pattern = new_pattern ~ parts[i] -%}
                    {%- endfor -%}
                    {%- set temp_pattern = new_pattern -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
        {# Step 2: Replace placeholders with capturing or non-capturing versions #}
        {%- set result_pattern = temp_pattern -%}
        {%- set group_num = 0 -%}
        {%- for config in parsed_columns -%}
            {%- set group_num = group_num + 1 -%}
            {%- set group_rgx = config.rgxExpression | default('') -%}
            {%- if group_rgx -%}
                {%- set placeholder = '__GROUP_' ~ group_num ~ '__' -%}
                {%- if group_num == target_group_index -%}
                    {%- set result_pattern = result_pattern | replace(placeholder, group_rgx) -%}
                {%- else -%}
                    {%- set non_cap_version = group_rgx | replace('(', '(?:') -%}
                    {%- set result_pattern = result_pattern | replace(placeholder, non_cap_version) -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- else -%}
        {# No duplicates: simple replace #}
        {%- set result_pattern = full_pattern -%}
        {%- set group_num = 0 -%}
        {%- for config in parsed_columns -%}
            {%- set group_num = group_num + 1 -%}
            {%- set group_rgx = config.rgxExpression | default('') -%}
            {%- if group_rgx and group_num != target_group_index -%}
                {%- set non_cap_version = group_rgx | replace('(', '(?:') -%}
                {%- set result_pattern = result_pattern | replace(group_rgx, non_cap_version) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- set final_pattern = ('(?i)' if case_insensitive else '') ~ result_pattern -%}
    {{ return(final_pattern) }}
{% endmacro %}

{# DuckDB-specific: Use full pattern (DuckDB regexp_extract supports multiple capturing groups) #}
{% macro duckdb__build_single_capture_pattern(full_pattern, parsed_columns, target_group_index, case_insensitive) %}
    {# DuckDB's regexp_extract supports multiple capturing groups, so we can use the full pattern #}
    {%- set final_pattern = ('(?i)' if case_insensitive else '') ~ full_pattern -%}
    {{ return(final_pattern) }}
{% endmacro %}

