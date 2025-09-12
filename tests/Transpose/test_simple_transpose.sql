-- Test Case: Simple transpose with minimal data
-- Functional scenario: Basic transpose operation with key columns
with actual as (
  {{ prophecy_basics.Transpose(
      relation_name=ref('simple_data'),
      keyColumns=['id'],
      dataColumns=['value1', 'value2'],
      nameColumn='metric',
      valueColumn='value'
  ) }}
),
expected as (
  select 1 as id, 'value1' as metric, '10' as value union all
  select 1 as id, 'value2' as metric, '20' as value union all
  select 2 as id, 'value1' as metric, '15' as value union all
  select 2 as id, 'value2' as metric, '25' as value union all
  select 3 as id, 'value1' as metric, '30' as value union all
  select 3 as id, 'value2' as metric, '40' as value
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
