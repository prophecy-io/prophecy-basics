-- Test Case: Transpose with no key columns
-- Functional scenario: Simple transpose without preserving key columns
with actual as (
  {{ prophecy_basics.Transpose(
      relation_name=ref('simple_data'),
      keyColumns=[],
      dataColumns=['value1', 'value2'],
      nameColumn='metric',
      valueColumn='value'
  ) }}
),
expected as (
  select 'value1' as metric, '10' as value union all
  select 'value1' as metric, '15' as value union all
  select 'value1' as metric, '30' as value union all
  select 'value2' as metric, '20' as value union all
  select 'value2' as metric, '25' as value union all
  select 'value2' as metric, '40' as value
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
