-- Test RecordID with special characters in column names
with actual as (
  {{ RecordID(
      relation_name=ref('special_chars_test_data'),
      method='auto_increment',
      record_id_column_name='record-id',
      incremental_id_type='integer',
      incremental_id_size=5,
      incremental_id_starting_val=100,
      generationMethod='tableLevel',
      position='first_column'
  ) }}
),
expected as (
  select 100 as "record-id", 1 as id, 'john-doe' as "user-name", 'admin|manager|user' as "data field", '2024-01-15' as "order_date", 'electronics|gadgets' as "product-category", 1500.50 as "sales amount"
  union all select 101, 2, 'jane-smith', 'user|guest', '2024-01-16', 'books|fiction', 25.99
  union all select 102, 3, 'bob-wilson', 'moderator|admin', '2024-01-17', 'clothing|shoes', 89.95
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
