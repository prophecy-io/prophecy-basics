-- Test Case: Split product categories with special character cleanup
-- Functional scenario: E-commerce category management with data cleaning
with actual as (
  {{ TextToColumns(
      relation_name=ref('product_categories'),
      columnName='categories',
      delimiter='|',
      split_strategy='splitRows',
      noOfColumns=0,
      leaveExtraCharLastCol=false,
      splitColumnPrefix='unused',
      splitColumnSuffix='unused',
      splitRowsColumnName='category'
  ) }}
),
expected as (
  select 1 as id, 'Gaming Laptop' as product_name, 'Electronics|Computers|Gaming_{Hardware}|Laptops' as categories, 'Electronics' as category union all
  select 1 as id, 'Gaming Laptop' as product_name, 'Electronics|Computers|Gaming_{Hardware}|Laptops' as categories, 'Computers' as category union all
  select 1 as id, 'Gaming Laptop' as product_name, 'Electronics|Computers|Gaming_{Hardware}|Laptops' as categories, 'Gaming Hardware' as category union all
  select 1 as id, 'Gaming Laptop' as product_name, 'Electronics|Computers|Gaming_{Hardware}|Laptops' as categories, 'Laptops' as category union all
  select 2 as id, 'Coffee Mug' as product_name, 'Kitchen|Drinkware|{Ceramic}_Items' as categories, 'Kitchen' as category union all
  select 2 as id, 'Coffee Mug' as product_name, 'Kitchen|Drinkware|{Ceramic}_Items' as categories, 'Drinkware' as category union all
  select 2 as id, 'Coffee Mug' as product_name, 'Kitchen|Drinkware|{Ceramic}_Items' as categories, 'Ceramic Items' as category union all
  select 3 as id, 'Running Shoes' as product_name, 'Sports|Footwear|Running_Gear' as categories, 'Sports' as category union all
  select 3 as id, 'Running Shoes' as product_name, 'Sports|Footwear|Running_Gear' as categories, 'Footwear' as category union all
  select 3 as id, 'Running Shoes' as product_name, 'Sports|Footwear|Running_Gear' as categories, 'Running Gear' as category
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
