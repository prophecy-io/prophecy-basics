-- Test UUID-based RecordID functionality
with actual as (
  {{ RecordID(
      relation_name=ref('record_id_sample_data'),
      method='uuid',
      record_id_column_name='unique_id',
      incremental_id_type='string',
      incremental_id_size=10,
      incremental_id_starting_val=1,
      generationMethod='tableLevel',
      position='last_column'
  ) }}
),
expected as (
  select 1 as id, 'John Doe' as name, 'Engineering' as department, 75000 as salary, '2023-01-15' as join_date, 'uuid_check' as unique_id
  union all select 2, 'Jane Smith', 'Marketing', 65000, '2023-02-20', 'uuid_check'
  union all select 3, 'Bob Johnson', 'Engineering', 80000, '2023-03-10', 'uuid_check'
  union all select 4, 'Alice Brown', 'HR', 60000, '2023-04-05', 'uuid_check'
  union all select 5, 'Charlie Wilson', 'Engineering', 85000, '2023-05-12', 'uuid_check'
)
select *
from (
  (select id, name, department, salary, join_date from actual except all select id, name, department, salary, join_date from expected)
  union all
  (select id, name, department, salary, join_date from expected except all select id, name, department, salary, join_date from actual)
) diff
