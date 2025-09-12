-- Test Case: Transpose sales data with multiple key columns
-- Functional scenario: Quarterly sales data analysis
with actual as (
  {{ prophecy_basics.Transpose(
      relation_name=ref('sales_data'),
      keyColumns=['id', 'product', 'region'],
      dataColumns=['q1_sales', 'q2_sales', 'q3_sales', 'q4_sales'],
      nameColumn='quarter',
      valueColumn='sales_amount'
  ) }}
),
expected as (
  select 1 as id, 'Laptop' as product, 'North' as region, 'q1_sales' as quarter, '1000' as sales_amount union all
  select 1 as id, 'Laptop' as product, 'North' as region, 'q2_sales' as quarter, '1200' as sales_amount union all
  select 1 as id, 'Laptop' as product, 'North' as region, 'q3_sales' as quarter, '1100' as sales_amount union all
  select 1 as id, 'Laptop' as product, 'North' as region, 'q4_sales' as quarter, '1300' as sales_amount union all
  select 2 as id, 'Phone' as product, 'South' as region, 'q1_sales' as quarter, '800' as sales_amount union all
  select 2 as id, 'Phone' as product, 'South' as region, 'q2_sales' as quarter, '900' as sales_amount union all
  select 2 as id, 'Phone' as product, 'South' as region, 'q3_sales' as quarter, '950' as sales_amount union all
  select 2 as id, 'Phone' as product, 'South' as region, 'q4_sales' as quarter, '1000' as sales_amount union all
  select 3 as id, 'Tablet' as product, 'East' as region, 'q1_sales' as quarter, '600' as sales_amount union all
  select 3 as id, 'Tablet' as product, 'East' as region, 'q2_sales' as quarter, '700' as sales_amount union all
  select 3 as id, 'Tablet' as product, 'East' as region, 'q3_sales' as quarter, '650' as sales_amount union all
  select 3 as id, 'Tablet' as product, 'East' as region, 'q4_sales' as quarter, '750' as sales_amount union all
  select 4 as id, 'Monitor' as product, 'West' as region, 'q1_sales' as quarter, '400' as sales_amount union all
  select 4 as id, 'Monitor' as product, 'West' as region, 'q2_sales' as quarter, '450' as sales_amount union all
  select 4 as id, 'Monitor' as product, 'West' as region, 'q3_sales' as quarter, '500' as sales_amount union all
  select 4 as id, 'Monitor' as product, 'West' as region, 'q4_sales' as quarter, '550' as sales_amount union all
  select 5 as id, 'Keyboard' as product, 'North' as region, 'q1_sales' as quarter, '200' as sales_amount union all
  select 5 as id, 'Keyboard' as product, 'North' as region, 'q2_sales' as quarter, '250' as sales_amount union all
  select 5 as id, 'Keyboard' as product, 'North' as region, 'q3_sales' as quarter, '300' as sales_amount union all
  select 5 as id, 'Keyboard' as product, 'North' as region, 'q4_sales' as quarter, '350' as sales_amount
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
