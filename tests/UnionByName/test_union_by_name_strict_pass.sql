-- Test: nameBasedUnionOperation (strict mode, schemas must match)
with actual as (
    {{ UnionByName(
        ['table_a', 'table_b_strict'],
        [
            [
                {"name": "id", "type": "int"},
                {"name": "value", "type": "string"}
            ],
            [
                {"name": "id", "type": "int"},
                {"name": "value", "type": "string"}
            ]
        ],
        missingColumnOps='nameBasedUnionOperation'
    ) }}
),

expected as (
    select 1 as id, 'foo' as value
    union all
    select 2 as id, 'bar' as value
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
