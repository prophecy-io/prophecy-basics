with actual as (
  {{ CountRecords(ref('total_records'), ['id','name']) }}
),
expected as (
  select 4 as total_records
)
select * from (
  select * from actual
  except all
  select * from expected
) diff