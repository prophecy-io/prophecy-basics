-- Test Case: MultiColumnEdit with special characters in column names
-- Functional scenario: Data transformation with columns containing hyphens and spaces
with actual_no_rename as (
  {{ MultiColumnEdit(
      relation=ref('special_chars_test_data'),
      expressionToBeApplied='UPPER(column_value)',
      allColumnNames=['id', 'user-name', 'data field', 'order_date'],
      columnNames=['user-name', 'data field'],
      changeOutputFieldName=false,
      prefixSuffixOption='prefix',
      prefixSuffixToBeAdded=''
  ) }}
),
expected_no_rename as (
  select 1 as id, 'JOHN-DOE' as "user-name", 'ADMIN|MANAGER|USER' as "data field", '2024-01-15' as order_date union all
  select 2 as id, 'JANE-SMITH' as "user-name", 'USER|GUEST' as "data field", '2024-01-16' as order_date union all
  select 3 as id, 'BOB-WILSON' as "user-name", 'MODERATOR|ADMIN' as "data field", '2024-01-17' as order_date
),
actual_with_prefix as (
  {{ MultiColumnEdit(
      relation=ref('special_chars_test_data'),
      expressionToBeApplied='LOWER(column_value)',
      allColumnNames=['id', 'user-name', 'data field'],
      columnNames=['user-name'],
      changeOutputFieldName=true,
      prefixSuffixOption='prefix',
      prefixSuffixToBeAdded='clean_'
  ) }}
),
expected_with_prefix as (
  select 1 as id, 'john-doe' as "user-name", 'admin|manager|user' as "data field",
         'john-doe' as "clean_user-name" union all
  select 2 as id, 'jane-smith' as "user-name", 'user|guest' as "data field",
         'jane-smith' as "clean_user-name" union all
  select 3 as id, 'bob-wilson' as "user-name", 'moderator|admin' as "data field",
         'bob-wilson' as "clean_user-name"
)
-- Test no rename scenario (transform in place)
select *
from (
  (select * from actual_no_rename except all select * from expected_no_rename)
  union all
  (select * from expected_no_rename except all select * from actual_no_rename)
) diff_no_rename

union all

-- Test with prefix scenario (create new columns)
select *
from (
  (select * from actual_with_prefix except all select * from expected_with_prefix)
  union all
  (select * from expected_with_prefix except all select * from actual_with_prefix)
) diff_with_prefix
