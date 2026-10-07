{% materialization truncate_insert, default %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set full_refresh_mode = should_full_refresh() -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set truncate_mode = existing_relation is not none
                          and existing_relation.is_table
                          and not full_refresh_mode -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if truncate_mode %}

    {%- do prophecy_basics.prophecy_truncate_relation(existing_relation) -%}

    {% call statement('main') -%}
      insert into {{ target_relation }}
      {{ sql }}
    {%- endcall %}

  {% else %}

    {% if existing_relation is not none %}
      {%- do adapter.drop_relation(existing_relation) -%}
    {% endif %}

    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}

  {% endif %}

  {%- do persist_docs(target_relation, model) -%}

  {%- set should_revoke = should_revoke(existing_relation, full_refresh_mode=not truncate_mode) -%}
  {%- do apply_grants(target_relation, grant_config, should_revoke=should_revoke) -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {%- do adapter.commit() -%}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro prophecy_truncate_relation(relation) %}
  {{ return(adapter.dispatch('prophecy_truncate_relation', 'prophecy_basics')(relation)) }}
{% endmacro %}


{% macro default__prophecy_truncate_relation(relation) %}
  {%- do adapter.truncate_relation(relation) -%}
{% endmacro %}


{% macro bigquery__prophecy_truncate_relation(relation) %}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation.render() }}
  {%- endcall %}
{% endmacro %}
