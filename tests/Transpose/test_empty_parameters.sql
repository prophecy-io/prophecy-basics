-- Test Case: Transpose with empty parameters
-- Functional scenario: Edge case handling - should return original data
with actual as (
  {{ prophecy_basics.Transpose(
      relation_name=ref('simple_data'),
      keyColumns=[],
      dataColumns=[],
      nameColumn='',
      valueColumn=''
  ) }}
),
expected as (
  select 1 as id, 'ItemA' as name, 10 as value1, 20 as value2 union all
  select 2 as id, 'ItemB' as name, 15 as value1, 25 as value2 union all
  select 3 as id, 'ItemC' as name, 30 as value1, 40 as value2
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
