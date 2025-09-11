-- Test Case: Parse addresses into street, city, state, zip
-- Functional scenario: Address standardization for shipping/billing
with actual as (
  {{ TextToColumns(
      relation_name=ref('addresses'),
      columnName='address',
      delimiter='|',
      split_strategy='splitColumns',
      noOfColumns=4,
      leaveExtraCharLastCol=true,
      splitColumnPrefix='addr',
      splitColumnSuffix='component',
      splitRowsColumnName='unused'
  ) }}
),
expected as (
  select 1 as id, '123 Main St|New York|NY|10001' as address, 
         '123 Main St' as addr_1_component, 'New York' as addr_2_component, 'NY' as addr_3_component, '10001' as addr_4_component union all
  select 2 as id, '456 Oak Ave|Los Angeles|CA|90210' as address,
         '456 Oak Ave' as addr_1_component, 'Los Angeles' as addr_2_component, 'CA' as addr_3_component, '90210' as addr_4_component union all
  select 3 as id, '789 Pine Rd|Chicago|IL' as address,
         '789 Pine Rd' as addr_1_component, 'Chicago' as addr_2_component, 'IL' as addr_3_component, NULL as addr_4_component union all
  select 4 as id, '321 Elm Dr|Miami|FL|33101|Apt 5B' as address,
         '321 Elm Dr' as addr_1_component, 'Miami' as addr_2_component, 'FL' as addr_3_component, '33101|Apt 5B' as addr_4_component
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
