-- Test Case: Split store locations using semicolon delimiter
-- Functional scenario: Retail analytics - store location analysis
with actual as (
  {{ TextToColumns(
      relation_name=ref('store_locations'),
      columnName='locations',
      delimiter=';',
      split_strategy='splitRows',
      noOfColumns=0,
      leaveExtraCharLastCol=false,
      splitColumnPrefix='unused',
      splitColumnSuffix='unused',
      splitRowsColumnName='location'
  ) }}
),
expected as (
  select 1 as id, 'Coffee Co' as store_chain, 'New York;Los Angeles;Chicago;Miami' as locations, 'New York' as location union all
  select 1 as id, 'Coffee Co' as store_chain, 'New York;Los Angeles;Chicago;Miami' as locations, 'Los Angeles' as location union all
  select 1 as id, 'Coffee Co' as store_chain, 'New York;Los Angeles;Chicago;Miami' as locations, 'Chicago' as location union all
  select 1 as id, 'Coffee Co' as store_chain, 'New York;Los Angeles;Chicago;Miami' as locations, 'Miami' as location union all
  select 2 as id, 'Tech Store' as store_chain, 'San Francisco;Seattle' as locations, 'San Francisco' as location union all
  select 2 as id, 'Tech Store' as store_chain, 'San Francisco;Seattle' as locations, 'Seattle' as location union all
  select 3 as id, 'Book Shop' as store_chain, 'Boston;Philadelphia;Washington DC;Atlanta;Denver' as locations, 'Boston' as location union all
  select 3 as id, 'Book Shop' as store_chain, 'Boston;Philadelphia;Washington DC;Atlanta;Denver' as locations, 'Philadelphia' as location union all
  select 3 as id, 'Book Shop' as store_chain, 'Boston;Philadelphia;Washington DC;Atlanta;Denver' as locations, 'Washington DC' as location union all
  select 3 as id, 'Book Shop' as store_chain, 'Boston;Philadelphia;Washington DC;Atlanta;Denver' as locations, 'Atlanta' as location union all
  select 3 as id, 'Book Shop' as store_chain, 'Boston;Philadelphia;Washington DC;Atlanta;Denver' as locations, 'Denver' as location
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
