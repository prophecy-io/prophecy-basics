-- Test Case: CountRecords with special characters in column names
-- Functional scenario: Data quality analysis with columns containing hyphens and spaces
with non_null_actual as (
  select * from (
    {{ CountRecords(
        relation_name=ref('special_chars_test_data'),
        column_names=['user-name', 'data field', 'sales amount'],
        count_method='count_non_null_records'
    ) }}
  ) t
),
non_null_expected as (
  select 3 as "user-name_count", 3 as "data field_count", 3 as "sales amount_count"
)
-- Test non-null count functionality with special characters
select 'non_null_test' as test_type, 
       "user-name_count", "data field_count", "sales amount_count",
       NULL as "user-name_distinct_count", NULL as "product-category_distinct_count"
from (
  (select * from non_null_actual except all select * from non_null_expected)
  union all
  (select * from non_null_expected except all select * from non_null_actual)
) diff_non_null

union all

-- Test distinct count functionality with special characters  
select 'distinct_test' as test_type,
       NULL as "user-name_count", NULL as "data field_count", NULL as "sales amount_count",
       "user-name_distinct_count", "product-category_distinct_count"
from (
  (select * from (
    {{ CountRecords(
        relation_name=ref('special_chars_test_data'),
        column_names=['user-name', 'product-category'],
        count_method='count_distinct_records'
    ) }}
  ) t except all select 3 as "user-name_distinct_count", 3 as "product-category_distinct_count")
  union all
  (select 3 as "user-name_distinct_count", 3 as "product-category_distinct_count" except all 
   select * from (
    {{ CountRecords(
        relation_name=ref('special_chars_test_data'),
        column_names=['user-name', 'product-category'],
        count_method='count_distinct_records'
    ) }}
   ) t)
) diff_distinct
