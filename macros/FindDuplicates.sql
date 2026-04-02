{#
  FindDuplicates Macro Gem
  ========================

  Finds duplicate rows or keeps unique rows within groups you define: it ranks
  rows by your keys and sort order, then returns only the slice you want—such as
  all but one copy, only duplicates, or rows whose group size matches a rule.

  Parameters:
    - relation_name (string or list): Source relation(s).
    - groupByColumnNames (list): PARTITION BY columns when generationMethod is not "allCols".
    - column_group_rownum_condition: "between" | "equal_to" | "not_equal_to" | "less_than" | "greater_than"
        (used with lower_limit/upper_limit or grouped_count_rownum as applicable).
    - output_type (string): "unique" (row_num = 1), "duplicate" (row_num > 1),
        "custom_row_number" (filter row_num), "custom_group_count" (filter group_count).
    - grouped_count_rownum, lower_limit, upper_limit: Thresholds for custom_* modes.
    - generationMethod (string): "allCols" — partition by all schema_columns; else use groupByColumnNames.
    - schema_columns (list): All column names for SELECT EXCEPT / partition-all mode.
    - orderByColumns (list[dict]): Same shape as Prophecy orderByColumns. Each item:
        { "expression": { "expression": "<SQL expression>" }, "sortType": "<sort>" }
        expression is raw SQL (e.g. `col1`, concat(`a`,`b`)). sortType: asc, desc, asc_nulls_last,
        desc_nulls_first (else treated as desc). Ignored when generationMethod is "allCols" (window uses ORDER BY 1).
        Empty list → ORDER BY 1 in the row_number window.

  Adapter Support:
    - default__ (row_number, * EXCEPT), snowflake__ (ROW_NUMBER, EXCLUDE), duckdb__ (quoted schema columns on filter)

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {{ prophecy_basics.FindDuplicates('src', ['id'], 'equal_to', 'duplicate', 1, 1, 1, 'groupBy', ['c1','c2'], []) }}
    {% set orderByColumns = [
      {'expression': {'expression': '`created_at`'}, 'sortType': 'desc'},
      {'expression': {'expression': 'concat(`c1`, `c2`)'}, 'sortType': 'asc_nulls_last'}
    ] %}
    {{ prophecy_basics.FindDuplicates('src', ['id'], 'equal_to', 'duplicate', 1, 1, 1, 'groupBy', ['c1','c2'], orderByColumns) }}

  CTE Usage Example:
    Macro call (first example — empty orderByColumns):
      {{ prophecy_basics.FindDuplicates('src', ['id'], 'equal_to', 'duplicate', 1, 1, 1, 'groupBy', ['c1','c2'], []) }}

    Resolved query (default__ — duplicate rows by id; ORDER BY 1):
      WITH select_cte1 AS (
          SELECT *, row_number() OVER (PARTITION BY `id` ORDER BY 1) AS row_num FROM src
      )
      SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num > 1

    Macro call (second example — orderByColumns with expression + sortType):
      {% set orderByColumns = [
        {'expression': {'expression': '`created_at`'}, 'sortType': 'desc'},
        {'expression': {'expression': 'concat(`c1`, `c2`)'}, 'sortType': 'asc_nulls_last'}
      ] %}
      {{ prophecy_basics.FindDuplicates('src', ['id'], 'equal_to', 'duplicate', 1, 1, 1, 'groupBy', ['c1','c2'], orderByColumns) }}

    Resolved query (default__):
      WITH select_cte1 AS (
          SELECT *, row_number() OVER (
              PARTITION BY `id`
              ORDER BY `created_at` desc, concat(`c1`, `c2`) asc nulls last
          ) AS row_num FROM src
      )
      SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num > 1
#}
{% macro FindDuplicates(relation_name,
    groupByColumnNames,
    column_group_rownum_condition,
    output_type,
    grouped_count_rownum,
    lower_limit,
    upper_limit,
    generationMethod,
    schema_columns,
    orderByColumns) -%}
    {{ return(adapter.dispatch('FindDuplicates', 'prophecy_basics')(relation_name,
    groupByColumnNames,
    column_group_rownum_condition,
    output_type,
    grouped_count_rownum,
    lower_limit,
    upper_limit,
    generationMethod,
    schema_columns,
    orderByColumns)) }}
{% endmacro %}


{%- macro default__FindDuplicates(
    relation_name,
    groupByColumnNames,
    column_group_rownum_condition,
    output_type,
    grouped_count_rownum,
    lower_limit,
    upper_limit,
    generationMethod,
    schema_columns,
    orderByColumns
) %}

    {{ log("Applying Window Function on selected columns", info=True) }}

    --{%- set partition_columns_str = groupByColumnNames | join(', ') -%}
    {%- set order_parts = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {%- if generationMethod == "allCols" -%}
        {%- do order_parts.append("1") -%}
    {%- else -%}
        {%- for r in orderByColumns -%}
            {%- if r.expression.expression | trim != '' -%}
                {%- set part = r.expression.expression | trim ~ " " -%}
                {%- if r.sortType == 'asc' -%}
                    {%- set part = part ~ "asc" -%}
                {%- elif r.sortType == 'asc_nulls_last' -%}
                    {%- set part = part ~ "asc nulls last" -%}
                {%- elif r.sortType == 'desc_nulls_first' -%}
                    {%- set part = part ~ "desc nulls first" -%}
                {%- else -%}
                    {%- set part = part ~ "desc" -%}
                {%- endif -%}
                {%- do order_parts.append(part) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set window_order_by_str = order_parts | join(', ')-%}

    {%- if window_order_by_str == '' -%}
        {%- set window_order_by_str_param = 'ORDER BY 1' -%}
    {%- else -%}
        {%- set window_order_by_str_param = 'ORDER BY ' ~ window_order_by_str -%}
    {%- endif -%}

    {# Quote column names for PARTITION BY clause #}
    {%- set quoted_column_names = [] -%}
    {%- for column in groupByColumnNames -%}
        {%- do quoted_column_names.append(prophecy_basics.quote_identifier(column)) -%}
    {%- endfor -%}

    {%- set quoted_schema_columns = [] -%}
    {%- for column in schema_columns -%}
        {%- do quoted_schema_columns.append(prophecy_basics.quote_identifier(column)) -%}
    {%- endfor -%}

    {%- set window_partition_by_str -%}
        {%- if generationMethod == "allCols" -%}
            PARTITION BY {{ quoted_schema_columns | join(', ') }}
        {%- else -%}
            PARTITION BY {{ quoted_column_names | join(', ') }}
        {% endif %}
    {% endset %}

    {%- set select_window_cte -%}
        {%- if output_type == "custom_group_count" -%}
            WITH select_cte1 AS(
                SELECT *, COUNT(*) OVER({{ window_partition_by_str }}) AS group_count FROM {{ relation_list | join(', ') }}
            )
        {%- else -%}
            WITH select_cte1 AS(
                SELECT *, row_number() OVER({{ window_partition_by_str }} {{ window_order_by_str_param }}) AS row_num FROM {{ relation_list | join(', ') }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set select_window_filter -%}
        {%- if output_type == "custom_group_count" -%}
            {%- if column_group_rownum_condition == "between" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count BETWEEN {{ lower_limit }} AND {{ upper_limit }}
            {%-elif column_group_rownum_condition == "equal_to" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count = {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "not_equal_to" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count <> {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "less_than" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count < {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "greater_than" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count > {{ grouped_count_rownum }}
            {%- endif -%}
        {%- elif output_type == "unique" -%}
            SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num = 1
        {%- elif output_type == "duplicate" -%}
            SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num > 1
        {%- elif output_type == "custom_row_number" -%}
            {%- if column_group_rownum_condition == "between" -%}
                SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num BETWEEN {{ lower_limit }} AND {{ upper_limit }}
            {%-elif column_group_rownum_condition == "equal_to" -%}
                SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num = {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "not_equal_to" -%}
                SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num <> {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "less_than" -%}
                SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num < {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "greater_than" -%}
                SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num > {{ grouped_count_rownum }}
            {%- endif -%}
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_window_cte ~ "\n" ~ select_window_filter -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro -%}

{%- macro snowflake__FindDuplicates(
    relation_name,
    groupByColumnNames,
    column_group_rownum_condition,
    output_type,
    grouped_count_rownum,
    lower_limit,
    upper_limit,
    generationMethod,
    schema_columns,
    orderByColumns
) -%}

    {{ log("Applying Window Function on selected columns (Snowflake)", info=True) }}

    {%- set order_parts = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {%- if generationMethod == "allCols" -%}
        {%- do order_parts.append("1") -%}
    {%- else -%}
        {%- for r in orderByColumns -%}
            {%- if r.expression.expression | trim != '' -%}
                {%- set part = r.expression.expression | trim ~ " " -%}
                {%- if r.sortType == 'asc' -%}
                    {%- set part = part ~ "asc" -%}
                {%- elif r.sortType == 'asc_nulls_last' -%}
                    {%- set part = part ~ "asc nulls last" -%}
                {%- elif r.sortType == 'desc_nulls_first' -%}
                    {%- set part = part ~ "desc nulls first" -%}
                {%- else -%}
                    {%- set part = part ~ "desc" -%}
                {%- endif -%}
                {%- do order_parts.append(part) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set window_order_by_str = order_parts | join(', ')-%}

    {%- if window_order_by_str == '' -%}
        {%- set window_order_by_str_param = 'ORDER BY 1' -%}
    {%- else -%}
        {%- set window_order_by_str_param = 'ORDER BY ' ~ window_order_by_str -%}
    {%- endif -%}

    {# Quote column names for PARTITION BY clause #}
    {%- set quoted_column_names = [] -%}
    {%- for column in groupByColumnNames -%}
        {%- do quoted_column_names.append(prophecy_basics.quote_identifier(column)) -%}
    {%- endfor -%}

    {%- set quoted_schema_columns = [] -%}
    {%- for column in schema_columns -%}
        {%- do quoted_schema_columns.append(prophecy_basics.quote_identifier(column)) -%}
    {%- endfor -%}

    {%- set window_partition_by_str -%}
        {%- if generationMethod == "allCols" -%}
            PARTITION BY {{ quoted_schema_columns | join(', ') }}
        {%- else -%}
            PARTITION BY {{ quoted_column_names | join(', ') }}
        {% endif %}
    {% endset %}

    {%- set select_window_cte -%}
        {%- if output_type == "custom_group_count" -%}
            WITH select_cte1 AS(
                SELECT *, COUNT(*) OVER({{ window_partition_by_str }}) AS group_count FROM {{ relation_list | join(', ') }}
            )
        {%- else -%}
            WITH select_cte1 AS(
                SELECT *, ROW_NUMBER() OVER({{ window_partition_by_str }} {{ window_order_by_str_param }}) AS row_num FROM {{ relation_list | join(', ') }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set select_window_filter -%}
        {%- if output_type == "custom_group_count" -%}
            {%- if column_group_rownum_condition == "between" -%}
                SELECT * EXCLUDE (group_count) FROM select_cte1 WHERE group_count BETWEEN {{ lower_limit }} AND {{ upper_limit }}
            {%-elif column_group_rownum_condition == "equal_to" -%}
                SELECT * EXCLUDE (group_count) FROM select_cte1 WHERE group_count = {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "not_equal_to" -%}
                SELECT * EXCLUDE (group_count) FROM select_cte1 WHERE group_count <> {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "less_than" -%}
                SELECT * EXCLUDE (group_count) FROM select_cte1 WHERE group_count < {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "greater_than" -%}
                SELECT * EXCLUDE (group_count) FROM select_cte1 WHERE group_count > {{ grouped_count_rownum }}
            {%- endif -%}
        {%- elif output_type == "unique" -%}
            SELECT * EXCLUDE (row_num) FROM select_cte1 WHERE row_num = 1
        {%- elif output_type == "duplicate" -%}
            SELECT * EXCLUDE (row_num) FROM select_cte1 WHERE row_num > 1
        {%- elif output_type == "custom_row_number" -%}
            {%- if column_group_rownum_condition == "between" -%}
                SELECT * EXCLUDE (row_num) FROM select_cte1 WHERE row_num BETWEEN {{ lower_limit }} AND {{ upper_limit }}
            {%-elif column_group_rownum_condition == "equal_to" -%}
                SELECT * EXCLUDE (row_num) FROM select_cte1 WHERE row_num = {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "not_equal_to" -%}
                SELECT * EXCLUDE (row_num) FROM select_cte1 WHERE row_num <> {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "less_than" -%}
                SELECT * EXCLUDE (row_num) FROM select_cte1 WHERE row_num < {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "greater_than" -%}
                SELECT * EXCLUDE (row_num) FROM select_cte1 WHERE row_num > {{ grouped_count_rownum }}
            {%- endif -%}
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_window_cte ~ "\n" ~ select_window_filter -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro -%}

{%- macro duckdb__FindDuplicates(
    relation_name,
    groupByColumnNames,
    column_group_rownum_condition,
    output_type,
    grouped_count_rownum,
    lower_limit,
    upper_limit,
    generationMethod,
    schema_columns,
    orderByColumns
) -%}

    {{ log("Applying Window Function on selected columns", info=True) }}

    --{%- set partition_columns_str = groupByColumnNames | join(', ') -%}
    {%- set order_parts = [] -%}
    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {%- if generationMethod == "allCols" -%}
        {%- do order_parts.append("1") -%}
    {%- else -%}
        {%- for r in orderByColumns -%}
            {%- if r.expression.expression | trim != '' -%}
                {%- set part = r.expression.expression | trim ~ " " -%}
                {%- if r.sortType == 'asc' -%}
                    {%- set part = part ~ "asc" -%}
                {%- elif r.sortType == 'asc_nulls_last' -%}
                    {%- set part = part ~ "asc nulls last" -%}
                {%- elif r.sortType == 'desc_nulls_first' -%}
                    {%- set part = part ~ "desc nulls first" -%}
                {%- else -%}
                    {%- set part = part ~ "desc" -%}
                {%- endif -%}
                {%- do order_parts.append(part) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set window_order_by_str = order_parts | join(', ')-%}

    {%- if window_order_by_str == '' -%}
        {%- set window_order_by_str_param = 'ORDER BY 1' -%}
    {%- else -%}
        {%- set window_order_by_str_param = 'ORDER BY ' ~ window_order_by_str -%}
    {%- endif -%}

    {# Quote column names for PARTITION BY clause #}
    {%- set quoted_column_names = [] -%}
    {%- for column in groupByColumnNames -%}
        {%- do quoted_column_names.append(prophecy_basics.quote_identifier(column)) -%}
    {%- endfor -%}

    {%- set quoted_schema_columns = [] -%}
    {%- for column in schema_columns -%}
        {%- do quoted_schema_columns.append(prophecy_basics.quote_identifier(column)) -%}
    {%- endfor -%}

    {%- set window_partition_by_str -%}
        {%- if generationMethod == "allCols" -%}
            PARTITION BY {{ quoted_schema_columns | join(', ') }}
        {%- else -%}
            PARTITION BY {{ quoted_column_names | join(', ') }}
        {% endif %}
    {% endset %}

    {%- set select_window_cte -%}
        {%- if output_type == "custom_group_count" -%}
            WITH select_cte1 AS(
                SELECT *, COUNT(*) OVER({{ window_partition_by_str }}) AS group_count FROM {{ relation_list | join(', ') }}
            )
        {%- else -%}
            WITH select_cte1 AS(
                SELECT *, row_number() OVER({{ window_partition_by_str }} {{ window_order_by_str_param }}) AS row_num FROM {{ relation_list | join(', ') }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set select_window_filter -%}
        {%- if output_type == "custom_group_count" -%}
            {%- if column_group_rownum_condition == "between" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE group_count BETWEEN {{ lower_limit }} AND {{ upper_limit }}
            {%-elif column_group_rownum_condition == "equal_to" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE group_count = {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "not_equal_to" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE group_count <> {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "less_than" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE group_count < {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "greater_than" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE group_count > {{ grouped_count_rownum }}
            {%- endif -%}
        {%- elif output_type == "unique" -%}
            SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE row_num = 1
        {%- elif output_type == "duplicate" -%}
            SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE row_num > 1
        {%- elif output_type == "custom_row_number" -%}
            {%- if column_group_rownum_condition == "between" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE row_num BETWEEN {{ lower_limit }} AND {{ upper_limit }}
            {%-elif column_group_rownum_condition == "equal_to" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE row_num = {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "not_equal_to" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE row_num <> {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "less_than" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE row_num < {{ grouped_count_rownum }}
            {%-elif column_group_rownum_condition == "greater_than" -%}
                SELECT {{ quoted_schema_columns | join(', ') }} FROM select_cte1 WHERE row_num > {{ grouped_count_rownum }}
            {%- endif -%}
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_window_cte ~ "\n" ~ select_window_filter -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro -%}
