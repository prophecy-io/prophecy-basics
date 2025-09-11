-- Test Case: Parse product data into 6 columns (more than available data)
-- Functional scenario: Product catalog import with variable data completeness
with actual as (
  {{ TextToColumns(
      relation_name=ref('product_data'),
      columnName='product_info',
      delimiter=',',
      split_strategy='splitColumns',
      noOfColumns=6,
      leaveExtraCharLastCol=true,
      splitColumnPrefix='product',
      splitColumnSuffix='field',
      splitRowsColumnName='unused'
  ) }}
),
expected as (
  select 1 as id, 'SKU001,Laptop,999.99,Electronics,Dell' as product_info,
         'SKU001' as product_1_field, 'Laptop' as product_2_field, '999.99' as product_3_field, 
         'Electronics' as product_4_field, 'Dell' as product_5_field, NULL as product_6_field union all
  select 2 as id, 'SKU002,Mouse,29.99' as product_info,
         'SKU002' as product_1_field, 'Mouse' as product_2_field, '29.99' as product_3_field,
         NULL as product_4_field, NULL as product_5_field, NULL as product_6_field union all
  select 3 as id, 'SKU003,Keyboard,79.99,Accessories' as product_info,
         'SKU003' as product_1_field, 'Keyboard' as product_2_field, '79.99' as product_3_field,
         'Accessories' as product_4_field, NULL as product_5_field, NULL as product_6_field
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
