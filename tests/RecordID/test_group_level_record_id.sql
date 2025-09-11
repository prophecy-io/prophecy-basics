-- Test group-level RecordID functionality
with actual as (
  {{ RecordID(
      relation_name=ref('record_id_sample_data'),
      method='auto_increment',
      record_id_column_name='dept_record_id',
      incremental_id_type='integer',
      incremental_id_size=5,
      incremental_id_starting_val=1,
      generationMethod='groupLevel',
      position='last_column',
      groupByColumnNames=['department']
  ) }}
),
expected as (
  select 1 as id, 'John Doe' as name, 'Engineering' as department, 75000 as salary, '2023-01-15' as join_date, 1 as dept_record_id
  union all select 2, 'Jane Smith', 'Marketing', 65000, '2023-02-20', 1
  union all select 3, 'Bob Johnson', 'Engineering', 80000, '2023-03-10', 2
  union all select 4, 'Alice Brown', 'HR', 60000, '2023-04-05', 1
  union all select 5, 'Charlie Wilson', 'Engineering', 85000, '2023-05-12', 3
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
