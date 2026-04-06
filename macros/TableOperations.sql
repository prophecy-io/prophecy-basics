{#
  TableOperations Macro Gem
  =========================

  Performs lifecycle actions on a Delta Lake table: register external data, vacuum
  old files, optimize and Z-order, restore a version or timestamp, delete or update
  rows, drop the table, repair metadata, or run custom DDL—then reports success or
  failure for auditing.

  Parameters:
    - catalog, database, tableName: Three-part name for target table.
    - action: 'registerTableInCatalog' | 'vacuumTable' | 'optimiseTable' | 'restoreTable' |
        'deleteFromTable' | 'updateTable' | 'dropTable' | 'fsckRepairTable' | 'runDDL'.
    - path: External DELTA location for registerTableInCatalog.
    - useExternalFilePath: unused in default__ snippet.
    - vacuumRetainNumHours: RETAIN clause for vacuum.
    - useOptimiseWhere, optimiseWhere, useOptimiseZOrder, optimiseZOrderColumns: OPTIMIZE options.
    - restoreVia: 'restoreViaVersion' | 'restoreViaTimestamp'; restoreValue: version or timestamp literal.
    - deleteCondition: WHERE for DELETE.
    - updateSetClause, updateCondition: UPDATE SET and optional WHERE.
    - runDDL: Custom DDL; {table_name} replaced with target_table.

  Adapter Support:
    - default__ (Databricks Delta); other adapters may override.

  Depends on schema parameter:
    No

  Macro Call Examples (default__):
    {{ prophecy_basics.TableOperations('cat', 'db', 't', 'vacuumTable', '', false, '168', false, '', false, '', '', '', 'true', '', '', '') }}
    {{ prophecy_basics.TableOperations('cat', 'db', 't', 'deleteFromTable', '', false, '', false, '', false, '', '', '', 'id < 0', '', '', '') }}

  CTE Usage Example:
    Macro call (first example above):
      {{ prophecy_basics.TableOperations('cat', 'db', 't', 'vacuumTable', '', false, '168', false, '', false, '', '', '', 'true', '', '', '') }}

    Resolved behavior (default__):
      -- run_query executes:
      VACUUM `cat`.`db`.`t` RETAIN 168 HOURS

    Resolved query (returned string from macro — what dbt materializes after the side-effect):
      SELECT 'vacuumTable' AS operation, '`cat`.`db`.`t`' AS target_table, 'Success' AS status, current_timestamp() AS executed_at
#}
{% macro TableOperations(
    catalog='',
    database='',
    tableName='',
    action='',
    path='',
    useExternalFilePath=false,
    vacuumRetainNumHours='',
    useOptimiseWhere=false,
    optimiseWhere='',
    useOptimiseZOrder=false,
    optimiseZOrderColumns='',
    restoreVia='',
    restoreValue='',
    deleteCondition='true',
    updateSetClause='',
    updateCondition='',
    runDDL=''    
) -%}
    {{ return(adapter.dispatch('TableOperations', 'prophecy_basics')(
        catalog,
        database,
        tableName,
        action,
        path,
        useExternalFilePath,
        vacuumRetainNumHours,
        useOptimiseWhere,
        optimiseWhere,
        useOptimiseZOrder,
        optimiseZOrderColumns,
        restoreVia,
        restoreValue,
        deleteCondition,
        updateSetClause,
        updateCondition,
        runDDL
    )) }}
{% endmacro %}

{% macro default__TableOperations(
    catalog='',
    database='',
    tableName='',
    action='',
    path='',
    useExternalFilePath=false,
    vacuumRetainNumHours='',
    useOptimiseWhere=false,
    optimiseWhere='',
    useOptimiseZOrder=false,
    optimiseZOrderColumns='',
    restoreVia='',
    restoreValue='',
    deleteCondition='true',
    updateSetClause='',
    updateCondition='',
    runDDL=''
) %}

    {%- set sql_command = '' -%}
    {%- set target_table = "`" ~ catalog ~ "`.`" ~ database ~ "`.`" ~ tableName ~ "`" -%}

    {#- Register Table in Catalog -#}
    {%- if action == 'registerTableInCatalog' -%}
        {%- set sql_command -%}
            CREATE TABLE {{ target_table }} USING DELTA LOCATION '{{ path }}'
        {%- endset -%}

    {#- Vacuum Table -#}
    {%- elif action == 'vacuumTable' -%}
        {%- if vacuumRetainNumHours != '' -%}
            {%- set sql_command -%}
                VACUUM {{ target_table }} RETAIN {{ vacuumRetainNumHours }} HOURS
            {%- endset -%}
        {%- else -%}
            {%- set sql_command -%}
                VACUUM {{ target_table }}
            {%- endset -%}
        {%- endif -%}

    {#- Optimize Table -#}
    {%- elif action == 'optimiseTable' -%}
        {%- set optimize_clause = 'OPTIMIZE ' ~ target_table -%}
        
        {%- if useOptimiseWhere -%}
            {%- set optimize_clause = optimize_clause ~ ' WHERE ' ~ optimiseWhere -%}
        {%- endif -%}
        
        {%- if useOptimiseZOrder -%}
            {%- set zorder_cols = optimiseZOrderColumns | trim -%}
            {%- set optimize_clause = optimize_clause ~ ' ZORDER BY (' ~ zorder_cols ~ ')' -%}
        {%- endif -%}
        
        {%- set sql_command = optimize_clause -%}

    {#- Restore Table -#}
    {%- elif action == 'restoreTable' -%}
        {%- if restoreVia == 'restoreViaVersion' -%}
            {%- set sql_command -%}
                RESTORE TABLE {{ target_table }} TO VERSION AS OF {{ restoreValue }}
            {%- endset -%}
        {%- elif restoreVia == 'restoreViaTimestamp' -%}
            {%- set sql_command -%}
                RESTORE TABLE {{ target_table }} TO TIMESTAMP AS OF '{{ restoreValue }}'
            {%- endset -%}
        {%- endif -%}

    {#- Delete from Table -#}
    {%- elif action == 'deleteFromTable' -%}
        {%- set sql_command -%}
            DELETE FROM {{ target_table }} WHERE {{ deleteCondition }}
        {%- endset -%}

    {#- Update Table -#}
    {%- elif action == 'updateTable' -%}
        {%- set update_cmd = 'UPDATE ' ~ target_table ~ ' SET ' -%}
        {%- set update_cmd = update_cmd ~ updateSetClause -%}
        
        {%- if updateCondition != '' -%}
            {%- set update_cmd = update_cmd ~ ' WHERE ' ~ updateCondition -%}
        {%- endif -%}
        
        {%- set sql_command = update_cmd -%}

    {#- Drop Table -#}
    {%- elif action == 'dropTable' -%}
        {%- set sql_command -%}
            DROP TABLE {{ target_table }}
        {%- endset -%}    

    {#- FSCK Repair Table -#}
    {%- elif action == 'fsckRepairTable' -%}
        {%- set sql_command -%}
            FSCK REPAIR TABLE {{ target_table }}
        {%- endset -%}

    {#- Run Custom DDL (modifies table) -#}
    {%- elif action == 'runDDL' -%}
        {%- set sql_command = runDDL | replace('{table_name}', target_table) -%}
    {%- endif -%}

    {#- Return the SQL command to be executed by dbt -#}
    {%- if sql_command != '' -%}
        {{ log("Table Operation - Action: " ~ action ~ " on Table: " ~ target_table, info=true) }}
        {{ log("SQL Command: " ~ sql_command, info=true) }}
        {% do run_query(sql_command) %}
        {{ return("SELECT '" ~ action ~ "' as operation, '" ~ target_table ~ "' as target_table, 'Success' as status, current_timestamp() as executed_at") }}
    {%- else -%}
        {{ log("No SQL command generated for action: " ~ action, info=true) }}
        {{ return("SELECT 'No Action' as operation, '" ~ target_table ~ "' as target_table, 'Failed' as status, current_timestamp() as executed_at") }}
    {%- endif -%}

{% endmacro %}
