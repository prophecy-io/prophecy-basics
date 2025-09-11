-- Test RecordID with custom ordering
with actual as (
  {{ RecordID(
      relation_name=ref('record_id_sample_data'),
      method='auto_increment',
      record_id_column_name='ordered_id',
      incremental_id_type='integer',
      incremental_id_size=5,
      incremental_id_starting_val=1,
      generationMethod='tableLevel',
      position='first_column',
      orderByRules=[
        {'expr': 'salary', 'sort': 'desc'},
        {'expr': 'name', 'sort': 'asc'}
      ]
  ) }}
),
expected as (
  select 1 as ordered_id, 5 as id, 'Charlie Wilson' as name, 'Engineering' as department, 85000 as salary, '2023-05-12' as join_date
  union all select 2, 3, 'Bob Johnson', 'Engineering', 80000, '2023-03-10'
  union all select 3, 1, 'John Doe', 'Engineering', 75000, '2023-01-15'
  union all select 4, 2, 'Jane Smith', 'Marketing', 65000, '2023-02-20'
  union all select 5, 4, 'Alice Brown', 'HR', 60000, '2023-04-05'
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
