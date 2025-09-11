-- Test: allowMissingColumns
with actual as (
    {{ UnionByName(
        ['table_a', 'table_b'],
        [
            [
                {"name": "id", "type": "int"},
                {"name": "value", "type": "string"}
            ],
            [
                {"name": "id", "type": "int"},
                {"name": "extra_col", "type": "string"}
            ]
        ],
        missingColumnOps='allowMissingColumns'
    ) }}
),

expected as (
    select 1 as id, 'foo' as value, null as extra_col
    union all
    select 2 as id, null as value, 'bar' as extra_col
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
