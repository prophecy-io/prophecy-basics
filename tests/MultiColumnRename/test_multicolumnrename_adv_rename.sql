-- tests/test_multicolumnrename_prefix.sql

with actual as (
    {{ MultiColumnRename(
        relation_name='rename_source',
        columnNames=['name'],
        renameMethod='advancedRename',
        schema=['id','name'],
        editType='',
        editWith='',
        customExpression=var('new_name')
    ) }}
),

expected as (
    select 1 as id, 'jack' as new_name_col, 'jack' as name
    union all
    select 2 as id, 'john' as new_name_col, 'john' as name
),

diff as (
    (select 'in_expected_not_actual' as diff_type, * from expected
     except
     select 'in_expected_not_actual', * from actual)
    union all
    (select 'in_actual_not_expected', * from actual
     except
     select 'in_actual_not_expected', * from expected)
)

select * from diff


