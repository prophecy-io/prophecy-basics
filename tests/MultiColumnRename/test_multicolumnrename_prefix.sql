-- tests/test_multicolumnrename_prefix.sql

with actual as (
    {{ MultiColumnRename(
        relation_name='rename_source',
        columnNames=['name'],
        renameMethod='editPrefixSuffix',
        schema=['id','name'],
        editType='Prefix',
        editWith='pre_'
    ) }}
),

expected as (
    select 1 as id, 'jack' as pre_name, 'jack' as name
    union all
    select 2 as id, 'john' as pre_name, 'john' as name
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
