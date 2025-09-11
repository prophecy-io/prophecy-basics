with actual as (
  {% set all_cols = ['id','country','first_name','age'] %}
  {{ MultiColumnEdit(
      relation=ref('people'),
      expressionToBeApplied='CAST(CAST(column_value AS DOUBLE) * 1.5 AS INT)',
      allColumnNames=all_cols,
      columnNames=['age'],
      changeOutputFieldName=true,
      prefixSuffixOption='suffix',
      prefixSuffixToBeAdded='_x'
  ) }}
),
-- Column order: originals, then age_x
expected as (
  select 1 as id, 'usa' as country, 'John' as first_name, 50 as age, 75 as age_x
  union all select 2, 'India', 'Ana', 40, 60
  union all select 3, 'norway', 'Ola', cast(null as int), cast(null as int)
  union all select 4, 'UK', 'li', 0, 0
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
