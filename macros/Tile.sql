{#
  Tile Macro Gem
  ==============

  Buckets rows into groups for analysis: equal-sized slices by row count or by
  running total of a measure, smart bands around the mean, one bucket per distinct
  value combination, or custom bins from numeric cutoffs.

  Parameters:
    - relation_name (list): Source relation identifier(s) (e.g. `['t']`); used raw in many branches after join.
    - schema (string): JSON-serialized upstream schema as stored on the Python properties object.
    - tile_method: 'equal_sum_tile' | 'equal_records_tile' | 'smart_tile' | 'unique_value_tile' | 'manual_tile'.
    - number_of_tiles, sum_column_name: For sum/record tiles.
    - orderByColumns (list[dict]): Window ORDER BY in the same shape as the Python property:
        { "expression": { "expression": "<SQL expression>", "format": "sql" }, "sortType": "<sortType>" }.
    - groupby_column_names, donot_split_tile_column_names: Group / do-not-split columns (comma-joined in SQL).
    - smart_tile_column_name, column_output_method_smartTile: Smart tile labeling verbosity.
    - unique_value_column_name: Columns for unique_value_tile.
    - manual_tile_column_name, manual_tiles_cutoff: Comma-separated cutoff string for manual_tile.

  Adapter Support:
    - default__ (Spark SQL functions); other adapters may define tile__ macros if present.

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {% set orderByColumns = [
      {'expression': {'expression': '`order_date`', 'format': 'sql'}, 'sortType': 'asc'},
      {'expression': {'expression': 'concat(`id`, `name`)', 'format': 'sql'}, 'sortType': 'desc_nulls_first'}
    ] %}
    {{ prophecy_basics.Tile(['t'], '[{"name": "id"}, {"name": "name"}, {"name": "amt"}]', 'equal_records_tile', 4, '', orderByColumns, ['g'], '', 'no_output_column_smartTile', [], '', '', []) }}

  CTE Usage Example:
    Macro call (orderByColumns uses the same expression + sortType shape as the Python property):
      {% set orderByColumns = [
        {'expression': {'expression': '`order_date`', 'format': 'sql'}, 'sortType': 'asc'},
        {'expression': {'expression': 'concat(`id`, `name`)', 'format': 'sql'}, 'sortType': 'desc_nulls_first'}
      ] %}
      {{ prophecy_basics.Tile(['t'], '[{"name": "id"}, {"name": "name"}, {"name": "amt"}]', 'equal_records_tile', 4, '', orderByColumns, ['g'], '', 'no_output_column_smartTile', [], '', '', []) }}

    Resolved query (default__ — equal_records_tile; illustrative window fragment; full statement depends on tile_method branch):
      -- Window ORDER BY from orderByColumns:
      --   PARTITION BY <group cols> ORDER BY `order_date` asc, concat(`id`, `name`) desc nulls first
      -- Typical shape: ntile / dense_rank over that ordering, then final SELECT with schema_cols, Tile_Num, ...
      -- Compile the macro in your project for the complete SQL.
#}
{% macro Tile(relation_name,
    schema,
    tile_method,
    number_of_tiles,
    sum_column_name,
    orderByColumns,
    groupby_column_names,
    smart_tile_column_name,
    column_output_method_smartTile,
    unique_value_column_name,
    manual_tile_column_name,
    manual_tiles_cutoff,
    donot_split_tile_column_names) -%}
    {{ return(adapter.dispatch('Tile', 'prophecy_basics')(relation_name,
    schema,
    tile_method,
    number_of_tiles,
    sum_column_name,
    orderByColumns,
    groupby_column_names,
    smart_tile_column_name,
    column_output_method_smartTile,
    unique_value_column_name,
    manual_tile_column_name,
    manual_tiles_cutoff,
    donot_split_tile_column_names)) }}
{% endmacro %}

{%- macro default__Tile(
    relation_name,
    schema,
    tile_method,
    number_of_tiles,
    sum_column_name,
    orderByColumns,
    groupby_column_names,
    smart_tile_column_name,
    column_output_method_smartTile,
    unique_value_column_name,
    manual_tile_column_name,
    manual_tiles_cutoff,
    donot_split_tile_column_names
) -%}

    {%- set schema_fields = fromjson(schema) if schema else [] -%}
    {%- set schema_col_parts = [] -%}
    {%- for field in schema_fields -%}
        {%- if field.name is defined and field.name | trim != '' -%}
            {%- do schema_col_parts.append(field.name) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set schema_cols = schema_col_parts | join(', ') -%}

    {% set relation_list = relation_name if relation_name is iterable and relation_name is not string else [relation_name] %}
    {% set relation_sql = relation_list | join(', ') %}
    {%- set isSumColInOrderBy = false -%}
    {%- set order_parts = [] -%}
    {%- for r in orderByColumns -%}
        {%- if r.expression.expression | trim != '' -%}
            {%- set part = r.expression.expression | trim ~ " " -%}
            {%- if part | trim == sum_column_name -%}
                {%- set isSumColInOrderBy = true -%}
            {% endif %}
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

    {%- set window_order_by_str = order_parts | join(', ')-%}

    {% set group_by_column = groupby_column_names | join(', ') %}
    {%- set no_split_column = donot_split_tile_column_names | join(', ') -%}
    {%- set window_stat = 'partition by ' ~ group_by_column -%}

    {%- if group_by_column == '' -%}
        {%- set window_stat = '' -%}
    {%- endif -%}

    {%- if tile_method == 'equal_sum_tile' -%}

        {% if number_of_tiles == '' or sum_column_name == '' %}
            select * from {{ relation_sql }}

        {% else %}
            with sorted_data as (
                select
                    *,
                    sum({{ sum_column_name }}) over (
                        {% if group_by_column %} partition by {{ group_by_column }} {% endif %}
                    ) as total_sum,
                    row_number() over (
                        {% if group_by_column %} partition by {{ group_by_column }} {% endif %}
                        order by {% if window_order_by_str == '' %} {{ sum_column_name }}
                                {% else %}
                                    {% if isSumColInOrderBy == true %} {{ window_order_by_str }} {% else %} {{ window_order_by_str }}, {{ sum_column_name }} asc {% endif %}
                                {% endif %}
                    ) as sort_pos
                from {{ relation_sql }}
            ),

            running_totals as (
                select
                    *,
                    sum({{ sum_column_name }}) over (
                        {% if group_by_column %} partition by {{ group_by_column }} {% endif %}
                        order by sort_pos
                        rows between unbounded preceding and current row
                    ) as running_sum
                from sorted_data
            ),

            prelim_tiles as (
                select
                    *,
                    ceil((running_sum / total_sum) * {{ number_of_tiles }}) as prelim_tile
                from running_totals
            ),

            distinct_tiles as (
                select distinct
                    prelim_tile
                    {% if group_by_column %}, {{ group_by_column }} {% endif %}
                from prelim_tiles
            ),

            tile_map as (
                select
                    prelim_tile,
                    {% if group_by_column %} {{ group_by_column }}, {% endif %}
                    row_number() over (
                        {% if group_by_column %}
                            partition by {{ group_by_column }}
                        {% endif %}
                        order by prelim_tile
                    ) as Tile_Num
                from distinct_tiles
            ),

            normalized_tiles as (
                select
                    p.*,
                    m.Tile_Num
                from prelim_tiles p
                join tile_map m
                    on p.prelim_tile = m.prelim_tile
                    {% if group_by_column %}
                        and p.{{ group_by_column }} = m.{{ group_by_column }}
                    {% endif %}
            ),

            final as (
                select
                    *,
                    sum({{ sum_column_name }}) over (
                        {% if group_by_column %} partition by {{ group_by_column }}, Tile_Num {% else %} partition by Tile_Num {% endif %}
                    ) as tile_sum,
                    row_number() over (
                        {% if group_by_column %} partition by {{ group_by_column }}, Tile_Num {% else %} partition by Tile_Num {% endif %}
                        order by {% if window_order_by_str %} {{ window_order_by_str }} {% else %} {{ schema_cols }} {% endif %}
                    ) as Tile_SequenceNum
                from normalized_tiles
            )

            select {{ schema_cols }}, Tile_Num, tile_sum, Tile_SequenceNum  from final order by {{ group_by_column }} {% if group_by_column%},{% endif %} Tile_Num
        {% endif %}

    {%- elif tile_method == 'equal_records_tile' -%}

        {% if number_of_tiles == '' %}
            select * from {{ relation_sql }}

        {% else %}
            with provisional as (
                select
                    *,
                    ntile({{ number_of_tiles }}) over({{ window_stat }} order by {% if window_order_by_str %} {{ window_order_by_str }} {% else %} {{ schema_cols }} {% endif %} ) as provisional_tile
                from {{ relation_sql }}
            ),

            {% if no_split_column %}
                grouped as (
                    select
                        *,
                        min(provisional_tile) over (
                            partition by {% if group_by_column %}
                                {{ group_by_column }},
                            {% endif %}
                            {{ no_split_column }}
                        ) as group_tile
                    from provisional
                ),
                renumbered as (
                    select
                        *,
                        {% if group_by_column %}
                            dense_rank() over (
                                partition by {{ group_by_column }}
                                order by group_tile
                            ) as Tile_Num
                        {% else %}
                            dense_rank() over (
                                order by group_tile
                            ) as Tile_Num
                        {% endif %}
                    from grouped
                ),
                final as (
                    select *
                    from renumbered
                )
            {% else %}
                final as (
                    select
                        *,
                        provisional_tile as Tile_Num
                    from provisional
                )
            {% endif %}

            SELECT {{ schema_cols }}, Tile_Num,
                    count(1) over(PARTITION BY
                                    {% if group_by_column %}
                                        {{ group_by_column }},
                                    {% endif %}
                                        Tile_Num
                                    ) as Tile_RecordCount,
                    row_number() over(PARTITION BY
                                    {% if group_by_column %}
                                        {{ group_by_column }},
                                    {% endif %}
                                        Tile_Num
                                        order by {% if window_order_by_str %} {{ window_order_by_str }} {% else %} {{ schema_cols }} {% endif %}
                                    ) as Tile_SequenceNum FROM final
        {% endif %}

    {%- elif tile_method == 'smart_tile' -%}
        {%- set output_name_col = 'SmartTile_Num' -%}
        {%- set output_tile_col = 'Tile_Num' -%}

        {% if smart_tile_column_name == '' %}
            select * from {{ relation_sql }}

        {% else %}
            with base as (
                select
                    *,
                    avg({{ smart_tile_column_name }}) over ({{ window_stat }}) as mean_val,
                    stddev_samp({{ smart_tile_column_name }}) over ({{ window_stat }}) as stddev_val
                from {{ relation_sql }}
            ),
            scored as (
                select
                    *,
                    case
                        when stddev_val = 0 then 0
                        else cast(floor( (({{ smart_tile_column_name }} - mean_val) / stddev_val) + 0.5) as int)
                    end as {{ output_tile_col }}
                from base
            )
            {% if column_output_method_smartTile != 'no_output_column_smartTile' %}
                , named as (
                    select
                        *,
                        case {{ output_tile_col }}
                            when 0  then 'Average'
                            when 1  then 'Above Average'
                            when 2  then 'High'
                            when 3  then 'Extremely High'
                            when -1 then 'Below Average'
                            when -2 then 'Low'
                            when -3 then 'Extremely Low'
                            else concat('Tile ', cast({{ output_tile_col }} as string))
                        end as SmartTile_BaseLabel,

                        case
                            when stddev_val is null or stddev_val = 0 then null
                            else mean_val + (cast({{ output_tile_col }} as double) - 0.5) * stddev_val
                        end as SmartTile_LowerBound,
                        case
                            when stddev_val is null or stddev_val = 0 then null
                            else mean_val + (cast({{ output_tile_col }} as double) + 0.5) * stddev_val
                        end as SmartTile_UpperBound
                    from scored
                ),
                final_named as (
                    select
                        n.*,
                        case
                            when '{{ column_output_method_smartTile }}' = 'output_verbose_column_smartTile'
                                then concat(
                                        SmartTile_BaseLabel,
                                        ' (',
                                        coalesce(cast(SmartTile_LowerBound as string), 'n/a'),
                                        ' to ',
                                        coalesce(cast(SmartTile_UpperBound as string), 'n/a'),
                                        ')'
                                    )
                            else SmartTile_BaseLabel
                        end as SmartTile_Num
                    from named n
                )
                select {{ schema_cols }}, {{ output_tile_col }}, SmartTile_Num,
                    row_number() over (
                        {% if window_stat != '' %}
                            {{ window_stat }},
                        {% else %}
                            partition by
                        {% endif %}
                        {{ output_tile_col }}
                        order by {{ smart_tile_column_name }}
                    ) as TileSequence_Num
                from final_named

            {% else %}
                select {{ schema_cols }}, {{ output_tile_col }},
                    row_number() over (
                        {% if window_stat != '' %}
                            {{ window_stat }},
                        {% else %}
                            partition by
                        {% endif %}
                        {{ output_tile_col }}
                        order by {{ smart_tile_column_name }}
                    ) as TileSequence_Num
                from scored
            {% endif %}

        {% endif %}

    {%- elif tile_method == 'unique_value_tile' -%}
        {%- set unique_tile_cols = unique_value_column_name | join(', ') -%}

        {% if unique_tile_cols == '' %}
            select * from {{ relation_sql }}

        {% else %}
            with unique_cte as(
                select *,
                    dense_rank() over (
                        {{ window_stat }} order by {{ unique_tile_cols }}
                    ) as Tile_Num
                    from {{ relation_sql }}

            ),
            final as (
                select *,
                    row_number() over (
                        {% if window_stat != '' %}
                            {{ window_stat }},
                        {% else %}
                            partition by
                        {% endif %}
                        Tile_Num order by {{ schema_cols }}
                    ) as Tile_SequenceNum
                from unique_cte
            )
            select * from final order by {{ group_by_column }} {% if group_by_column %}, {% endif %} {{ unique_tile_cols }}

        {% endif %}

    {%- elif tile_method == 'manual_tile' -%}
        {% set cutoff_list = manual_tiles_cutoff.split(',') if manual_tiles_cutoff else [] %}
        {% set cutoff_list = cutoff_list | map('trim') | list %}
        {% set cutoff_list = cutoff_list | map('float') | list %}

        {% if (cutoff_list | length == 0) or manual_tile_column_name == '' %}
            select * from {{ relation_sql }}

        {% else %}
            with base as (
                select
                    *,
                    case
                        {% for i in range(cutoff_list | length) %}
                            when cast({{ manual_tile_column_name }} as float) <= {{ cutoff_list[i] }} then {{ i }} + 1
                        {% endfor %}
                        else {{ cutoff_list | length }} + 1
                    end as Tile_Num
                from {{ relation_sql }}
            ),
            final as (
                select *,
                    row_number() over (
                        {% if window_stat == '' %}
                            partition by Tile_Num
                        {% else %}
                            {{ window_stat }}, Tile_Num
                        {% endif %}
                        order by {{ schema_cols }}
                    ) as Tile_SequenceNum
                from base
            )
            select * from final
        {% endif %}

    {% endif %}

{%- endmacro -%}
