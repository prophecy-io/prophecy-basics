{#
  Tile Macro Gem
  ==============

  Buckets rows into groups for analysis: equal-sized slices by row count or by
  running total of a measure, smart bands around the mean, one bucket per distinct
  value combination, or custom bins from numeric cutoffs.

  Parameters:
    - relation_name: Source table identifier (string; used raw in many branches).
    - tile_method: 'equal_sum_tile' | 'equal_records_tile' | 'smart_tile' | 'unique_value_tile' | 'manual_tile'.
    - num_tiles, sum_column: For sum/record tiles.
    - orderByRules: List of {expr, sort} for window ordering.
    - groupby_column_names, no_split_column_list: Group / do-not-split columns (comma-joined in SQL).
    - tile_column, column_output_method_smartTile: Smart tile labeling verbosity.
    - unique_value_column_name: Columns for unique_value_tile.
    - manual_tile_column_name, manual_tiles_cutoff: List of cutoffs for manual_tile.
    - schema_cols: Column list fragment for SELECT / ORDER BY in generated SQL.

  Adapter Support:
    - default__ (Spark SQL functions); other adapters may define tile__ macros if present.

  Depends on schema parameter:
    No

  Macro Call Example (default__ — schema_cols is a comma-separated column list string; pass real orderByRules from the app):
    {{ prophecy_basics.Tile('t', 'equal_records_tile', 4, '', order_by_rules, ['g'], '', '', 'no_output_column_smartTile', [], '', [], '', 'id, name, amt') }}

  CTE Usage Example:
    Macro call (see "Macro Call Example" above):
      {{ prophecy_basics.Tile('t', 'equal_records_tile', 4, '', order_by_rules, ['g'], '', '', 'no_output_column_smartTile', [], '', [], '', 'id, name, amt') }}

    Resolved query (default__ — equal_records_tile branches use ntile, dense_rank, and final SELECT with Tile_Num; exact SQL depends on orderByRules and no_split_column_list):
      -- Illustrative core: WITH provisional AS (SELECT *, ntile(4) OVER (PARTITION BY g ORDER BY ...) AS provisional_tile FROM t), ...
      -- Final SELECT includes schema columns, Tile_Num, Tile_RecordCount, Tile_SequenceNum.
      -- For the full statement, compile the macro in your project.
#}
{% macro Tile(relation_name,
    tile_method,
    num_tiles,
    sum_column,
    orderByRules,
    groupby_column_names,
    tile_column,
    column_output_method_smartTile,
    unique_value_column_name,
    manual_tile_column_name,
    manual_tiles_cutoff,
    no_split_column_list,
    schema_cols   ) -%}
    {{ return(adapter.dispatch('Tile', 'prophecy_basics')(relation_name,
    tile_method,
    num_tiles,
    sum_column,
    orderByRules,
    groupby_column_names,
    tile_column,
    column_output_method_smartTile,
    unique_value_column_name,
    manual_tile_column_name,
    manual_tiles_cutoff,
    no_split_column_list,
    schema_cols   )) }}
{% endmacro %}

{%- macro default__Tile(
    relation_name,
    tile_method,
    num_tiles,
    sum_column,
    orderByRules,
    groupby_column_names,
    tile_column,
    column_output_method_smartTile,
    unique_value_column_name,
    manual_tile_column_name,
    manual_tiles_cutoff,
    no_split_column_list,
    schema_cols
) -%}

    {%- set isSumColInOrderBy = false -%}
    {%- set order_parts = [] -%}
    {%- for r in orderByRules -%}
        {%- if r.expr | trim != '' -%}
            {%- set part = r.expr | trim ~ " " -%}
            {%- if part | trim == sum_column -%}
                {%- set isSumColInOrderBy = true -%}
            {% endif %}
            {%- if r.sort == 'asc' -%}
                {%- set part = part ~ "asc" -%}
            {%- elif r.sort == 'asc_nulls_last' -%}
                {%- set part = part ~ "asc nulls last" -%}
            {%- elif r.sort == 'desc_nulls_first' -%}
                {%- set part = part ~ "desc nulls first" -%}
            {%- else -%}
                {%- set part = part ~ "desc" -%}
            {%- endif -%}
            {%- do order_parts.append(part) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set window_order_by_str = order_parts | join(', ')-%}

    {% set group_by_column = groupby_column_names | join(', ') %}
    {%- set no_split_column = no_split_column_list | join(', ') -%}
    {%- set window_stat = 'partition by ' ~ group_by_column -%}

    {%- if group_by_column == '' -%}
        {%- set window_stat = '' -%}
    {%- endif -%}

    {%- if tile_method == 'equal_sum_tile' -%}

        {% if num_tiles == '' or sum_column == '' %}
            select * from {{ relation_name }}

        {% else %}
            with sorted_data as (
                select
                    *,
                    sum({{ sum_column }}) over (
                        {% if group_by_column %} partition by {{ group_by_column }} {% endif %}
                    ) as total_sum,
                    row_number() over (
                        {% if group_by_column %} partition by {{ group_by_column }} {% endif %}
                        order by {% if window_order_by_str == '' %} {{ sum_column }}
                                {% else %}
                                    {% if isSumColInOrderBy == true %} {{ window_order_by_str }} {% else %} {{ window_order_by_str }}, {{ sum_column }} asc {% endif %}
                                {% endif %}
                    ) as sort_pos
                from {{ relation_name }}
            ),

            running_totals as (
                select
                    *,
                    sum({{ sum_column }}) over (
                        {% if group_by_column %} partition by {{ group_by_column }} {% endif %}
                        order by sort_pos
                        rows between unbounded preceding and current row
                    ) as running_sum
                from sorted_data
            ),

            prelim_tiles as (
                select
                    *,
                    ceil((running_sum / total_sum) * {{ num_tiles }}) as prelim_tile
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
                    sum({{ sum_column }}) over (
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

        {% if num_tiles == '' %}
            select * from {{ relation_name }}

        {% else %}
            with provisional as (
                select
                    *,
                    ntile({{ num_tiles }}) over({{ window_stat }} order by {% if window_order_by_str %} {{ window_order_by_str }} {% else %} {{ schema_cols }} {% endif %} ) as provisional_tile
                from {{ relation_name }}
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

        {% if tile_column == '' %}
            select * from {{ relation_name }}

        {% else %}
            with base as (
                select
                    *,
                    avg({{ tile_column }}) over ({{ window_stat }}) as mean_val,
                    stddev_samp({{ tile_column }}) over ({{ window_stat }}) as stddev_val
                from {{ relation_name }}
            ),
            scored as (
                select
                    *,
                    case
                        when stddev_val = 0 then 0
                        else cast(floor( (({{ tile_column }} - mean_val) / stddev_val) + 0.5) as int)
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
                        order by {{ tile_column }}
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
                        order by {{ tile_column }}
                    ) as TileSequence_Num
                from scored
            {% endif %}

        {% endif %}

    {%- elif tile_method == 'unique_value_tile' -%}
        {%- set unique_tile_cols = unique_value_column_name | join(', ') -%}

        {% if unique_tile_cols == '' %}
            select * from {{ relation_name }}

        {% else %}
            with unique_cte as(
                select *,
                    dense_rank() over (
                        {{ window_stat }} order by {{ unique_tile_cols }}
                    ) as Tile_Num
                    from {{ relation_name }}

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
        {% set cutoff_list = manual_tiles_cutoff | map('trim') | list %}
        {% set cutoff_list = cutoff_list | map('float') | list %}

        {% if (cutoff_list | length == 0) or manual_tile_column_name == '' %}
            select * from {{ relation_name }}

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
                from {{ relation_name }}
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
