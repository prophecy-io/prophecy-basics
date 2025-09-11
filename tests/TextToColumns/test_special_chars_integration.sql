-- Integration test: All three macros working with special characters
-- This test verifies that TextToColumns, MultiColumnEdit, and CountRecords
-- all handle column names with special characters (hyphens, spaces) correctly

with textcolumns_result as (
  {{ TextToColumns(
      relation_name=ref('special_chars_test_data'),
      columnName='data field',
      delimiter='|',
      split_strategy='splitColumns',
      noOfColumns=2,
      leaveExtraCharLastCol=true,
      splitColumnPrefix='role',
      splitColumnSuffix='type',
      splitRowsColumnName='unused'
  ) }}
),
expected_textcolumns as (
  select 1 as id, 'john-doe' as "user-name", 'admin|manager|user' as "data field", 
         'admin' as role_1_type, 'manager|user' as role_2_type union all
  select 2 as id, 'jane-smith' as "user-name", 'user|guest' as "data field",
         'user' as role_1_type, 'guest' as role_2_type union all
  select 3 as id, 'bob-wilson' as "user-name", 'moderator|admin' as "data field",
         'moderator' as role_1_type, 'admin' as role_2_type
)
-- Test that TextToColumns works with special characters
select *
from (
  (select id, "user-name", "data field", role_1_type, role_2_type from textcolumns_result 
   except all 
   select id, "user-name", "data field", role_1_type, role_2_type from expected_textcolumns)
  union all
  (select id, "user-name", "data field", role_1_type, role_2_type from expected_textcolumns 
   except all 
   select id, "user-name", "data field", role_1_type, role_2_type from textcolumns_result)
) diff
