with actual as (
  {{ CountRecords(ref('non_null_counts'), ['id','name'], 'count_non_null_records') }}
),
expected as (
  select 2 as id_count, 2 as name_count
)
-- return rows = failures
select *
from (
  select * from actual
  except all
  select * from expected
) diff