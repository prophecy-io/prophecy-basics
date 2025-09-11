-- Test Case: Split customer full names into first and last name
-- Functional scenario: Customer data processing
with actual as (
  {{ TextToColumns(
      relation_name=ref('customer_names'),
      columnName='full_name',
      delimiter=' ',
      split_strategy='splitColumns',
      noOfColumns=2,
      leaveExtraCharLastCol=true,
      splitColumnPrefix='name',
      splitColumnSuffix='part',
      splitRowsColumnName='unused'
  ) }}
),
expected as (
  select 1 as id, 'John Smith' as full_name, 'John' as name_1_part, 'Smith' as name_2_part union all
  select 2 as id, 'Mary Jane Watson' as full_name, 'Mary' as name_1_part, 'Jane Watson' as name_2_part union all
  select 3 as id, 'Robert' as full_name, 'Robert' as name_1_part, NULL as name_2_part union all
  select 4 as id, 'Anna Maria Rodriguez Garcia' as full_name, 'Anna' as name_1_part, 'Maria Rodriguez Garcia' as name_2_part union all
  select 5 as id, NULL as full_name, NULL as name_1_part, NULL as name_2_part union all
  select 6 as id, NULL as full_name, NULL as name_1_part, NULL as name_2_part
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
