with actual as (
  {{ CountRecords(ref('all_null_id'), ['id','name'], 'count_non_null_records') }}
),
expected as (
  -- id is all NULL => 0; name has two non-nulls => 2
  select 0 as id_count, 2 as name_count
)
select * from (
  select * from actual
  except all
  select * from expected
) diff