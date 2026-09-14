{#
  truncate_insert Materialization
  ===============================

  A "table"-like materialization that refreshes a model by emptying the existing
  table and inserting into it, instead of dropping and recreating it.

  Because the table object is never replaced, everything attached to it survives
  a run: grants, table/column comments, table properties (e.g. Delta
  `columnMapping.mode` field ids), constraints, and dependent views.

  Usage:
    {{ config(materialized = 'truncate_insert') }}

    select ...

  Behavior:
    - Target does not exist          -> CREATE TABLE AS (adapter's create_table_as)
    - Target exists as a table       -> TRUNCATE, then INSERT INTO <table> <query>
    - Target exists as a view        -> dropped and recreated as a table
    - `--full-refresh`               -> dropped and recreated (this is how you
                                        reshape the table; needs DROP rights)

  Schema drift:
    Nothing is validated up front. The insert is a plain positional
    `insert into <table> <model query>`, so the warehouse rejects a drifted
    model the same way it would reject any hand-written insert. Use
    `--full-refresh` to rebuild the table in the new shape.

  Atomicity:
    On adapters with DDL/DML transactions (Postgres, Redshift, Snowflake) the
    truncate and insert commit together. On Databricks/Spark and BigQuery they
    do not, so a failed insert leaves the table empty and readers can observe
    an empty table between the two statements.

  Adapter support:
    - default__  -> every adapter whose `adapter.truncate_relation()` dispatches
                    to the `truncate_relation` macro, i.e. anything built on
                    dbt's SQLAdapter (Snowflake, Databricks, Redshift, Postgres,
                    Spark, DuckDB, ...).
    - bigquery   -> separate materialization below. BigQueryAdapter extends
                    BaseAdapter rather than SQLAdapter and hard-raises from
                    `truncate_relation()`, so the default body cannot be reused.
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

    {%- do adapter.truncate_relation(existing_relation) -%}

    {% call statement('main') -%}
      insert into {{ target_relation }}
      {{ sql }}
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
  BigQuery
  ========

  Same contract as the default, with two BigQuery-specific departures:

  1. The TRUNCATE is issued as SQL here instead of through
     `adapter.truncate_relation()`. BigQuery supports the `TRUNCATE TABLE` DDL
     perfectly well; it is only the adapter *method* that is missing, because
     BigQueryAdapter extends BaseAdapter (which raises) rather than SQLAdapter
     (which dispatches to the `truncate_relation` macro). Calling it yields:
         NotImplementedError: `truncate` is not implemented for this adapter!
     A `bigquery__truncate_relation` macro would not help -- nothing ever
     dispatches to it.

  2. BigQuery has no DDL/DML transactions, so hooks run unwrapped and there is
     no `adapter.commit()`, matching dbt-bigquery's own `table` and `incremental`
     materializations.
#}
{% materialization truncate_insert, adapter='bigquery' %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set full_refresh_mode = should_full_refresh() -%}
  {%- set grant_config = config.get('grants') -%}

  {#-- Only an existing *table* can be emptied in place. --#}
  {%- set truncate_mode = existing_relation is not none
                          and existing_relation.is_table
                          and not full_refresh_mode -%}

  {{ run_hooks(pre_hooks) }}

  {% if truncate_mode %}

    {#-- Partitioning and clustering are attached to the table object, so they
         survive this the same way grants and descriptions do. --#}
    {% call statement('truncate_relation') -%}
      truncate table {{ existing_relation.render() }}
    {%- endcall %}

    {% call statement('main') -%}
      insert into {{ target_relation.render() }}
      {{ sql }}
    {%- endcall %}

  {% else %}

    {%- set partition_by = adapter.parse_partition_by(config.get('partition_by', none)) -%}
    {%- set cluster_by = config.get('cluster_by', none) -%}

    {#-- `create_table_as` emits `create or replace table`, which already stands
         in for a drop when the existing object is a table BigQuery considers
         replaceable. A view, or a table whose partitioning/clustering changed,
         has to be dropped first. --#}
    {% if existing_relation is not none
          and (not existing_relation.is_table
               or not adapter.is_replaceable(existing_relation, partition_by, cluster_by)) %}
      {%- do adapter.drop_relation(existing_relation) -%}
    {% endif %}

    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}

  {% endif %}

  {{ run_hooks(post_hooks) }}

  {#-- In truncate mode the object is not replaced, so grants carry over and we
       only apply the diff. --#}
  {%- set should_revoke = should_revoke(existing_relation, full_refresh_mode=not truncate_mode) -%}
  {%- do apply_grants(target_relation, grant_config, should_revoke=should_revoke) -%}

  {%- do persist_docs(target_relation, model) -%}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
