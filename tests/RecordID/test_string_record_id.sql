-- Test string-based RecordID functionality
with actual as (
  {{ RecordID(
      relation_name=ref('record_id_sample_data'),
      method='auto_increment',
      record_id_column_name='record_code',
      incremental_id_type='string',
      incremental_id_size=6,
      incremental_id_starting_val=1,
      generationMethod='tableLevel',
      position='first_column'
  ) }}
),
expected as (
  select '000001' as record_code, 1 as id, 'John Doe' as name, 'Engineering' as department, 75000 as salary, '2023-01-15' as join_date
  union all select '000002', 2, 'Jane Smith', 'Marketing', 65000, '2023-02-20'
  union all select '000003', 3, 'Bob Johnson', 'Engineering', 80000, '2023-03-10'
  union all select '000004', 4, 'Alice Brown', 'HR', 60000, '2023-04-05'
  union all select '000005', 5, 'Charlie Wilson', 'Engineering', 85000, '2023-05-12'
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
