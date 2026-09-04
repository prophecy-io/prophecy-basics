{#
  truncate_insert Materialization
  ===============================

  A "table"-like materialization that refreshes a model by emptying the existing
  table and re-inserting into it, instead of dropping and recreating it.

  Because the table object is never replaced, everything attached to it survives
  a run: grants, table/column comments, table properties (e.g. Delta
  `columnMapping.mode` field ids), constraints, and dependent views.

  Usage:
    {{ config(materialized = 'truncate_insert') }}

    select ...

  Behavior:
    - Target does not exist          -> CREATE TABLE AS (adapter's create_table_as)
    - Target exists as a table       -> TRUNCATE, then INSERT INTO ... SELECT
    - Target exists as a view        -> dropped and recreated as a table
    - `--full-refresh`               -> dropped and recreated (schema drift is
                                        fixed here; note this needs DROP rights)

  Column handling:
    The insert lists the destination columns explicitly and selects them *by
    name* out of the model query, so a re-ordered SELECT still lands in the
    right columns. A column present in the table but missing from the model
    query is a compile-time error; a column produced by the model but absent
    from the table is ignored. Use `--full-refresh` to reshape the table.

  Atomicity:
    On adapters with DDL/DML transactions (Postgres, Redshift, Snowflake) the
    truncate and insert commit together. On Databricks/Spark and BigQuery they
    do not, so readers can observe an empty table between the two statements.
    Override `prophecy_truncate_insert_sql` for a single-statement replace
    (e.g. Databricks `INSERT OVERWRITE`) where that matters.

  Adapter Support:
    - default__ (any adapter whose `TRUNCATE TABLE` is implemented, which is
      every adapter deriving from SQLAdapter: Databricks/Spark, Snowflake,
      Postgres, Redshift, BigQuery, DuckDB)
    - Adapters without TRUNCATE (e.g. Athena) can override
      `prophecy_empty_relation` with `delete from`.
#}

{% materialization truncate_insert, default %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set full_refresh_mode = should_full_refresh() -%}
  {%- set grant_config = config.get('grants') -%}

  {#-- Only an existing *table* can be emptied in place. --#}
  {%- set truncate_mode = existing_relation is not none
                          and existing_relation.is_table
                          and not full_refresh_mode -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {#-- `BEGIN` happens here on adapters that support transactions. --#}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if truncate_mode %}

    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- if dest_columns | length == 0 -%}
      {%- do exceptions.raise_compiler_error(
            "truncate_insert: could not read any columns from existing relation "
            ~ existing_relation ~ ". Run with --full-refresh to rebuild it.") -%}
    {%- endif -%}

    {#-- Fail loudly, and before touching the table, on schema drift. --#}
    {%- set model_columns = get_columns_in_query(sql) -%}
    {%- set model_columns_lower = model_columns | map('lower') | list -%}
    {%- set missing = [] -%}
    {%- for col in dest_columns -%}
      {%- if col.name | lower not in model_columns_lower -%}
        {%- do missing.append(col.name) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if missing | length > 0 -%}
      {%- do exceptions.raise_compiler_error(
            "truncate_insert: model '" ~ model.name ~ "' does not produce column(s) "
            ~ missing | join(', ') ~ " present in " ~ existing_relation
            ~ ". Add them to the model, or run with --full-refresh to reshape the table.") -%}
    {%- endif -%}

    {%- do prophecy_basics.prophecy_empty_relation(existing_relation) -%}

    {% call statement('main') -%}
      {{ prophecy_basics.prophecy_truncate_insert_sql(target_relation, dest_columns, sql) }}
    {%- endcall %}

  {% else %}

    {% if existing_relation is not none %}
      {#-- A view can't be truncated, and --full-refresh means replace. Drop
           first so adapters without `create or replace` across relation types
           don't choke. --#}
      {%- do adapter.drop_relation(existing_relation) -%}
    {% endif %}

    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}

  {% endif %}

  {%- do persist_docs(target_relation, model) -%}

  {#-- In truncate mode the object is not replaced, so grants carry over and we
       only apply the diff. --#}
  {%- set should_revoke = should_revoke(existing_relation, full_refresh_mode=not truncate_mode) -%}
  {%- do apply_grants(target_relation, grant_config, should_revoke=should_revoke) -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {%- do adapter.commit() -%}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{#
  Empties `relation` in place. Named `prophecy_empty_relation` rather than
  `truncate_relation` to avoid colliding with dbt-core's global macro of that
  name.
#}
{% macro prophecy_empty_relation(relation) -%}
  {{ return(adapter.dispatch('prophecy_empty_relation', 'prophecy_basics')(relation)) }}
{%- endmacro %}

{% macro default__prophecy_empty_relation(relation) -%}
  {#-- Delegates to the adapter's own TRUNCATE implementation. --#}
  {%- do adapter.truncate_relation(relation) -%}
{%- endmacro %}


{#
  Builds the INSERT that repopulates the (now empty) target.
#}
{% macro prophecy_truncate_insert_sql(target_relation, dest_columns, select_sql) -%}
  {{ return(adapter.dispatch('prophecy_truncate_insert_sql', 'prophecy_basics')(
       target_relation, dest_columns, select_sql)) }}
{%- endmacro %}

{% macro default__prophecy_truncate_insert_sql(target_relation, dest_columns, select_sql) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
  insert into {{ target_relation }} ({{ dest_cols_csv }})
  select {{ dest_cols_csv }}
  from (
    {{ select_sql }}
  ) as __prophecy_truncate_insert_subq
{%- endmacro %}