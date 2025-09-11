with actual as (
  {{ CountRecords(ref('distinct_counts'), ['id','name'], 'count_distinct_records') }}
),
expected as (
  -- DISTINCT ignores NULLs; id ∈ {1,2} => 2 ; name ∈ {'a','b'} => 2
  select 2 as id_distinct_count, 2 as name_distinct_count
)
select * from (
  select * from actual
  except all
  select * from expected
) diff