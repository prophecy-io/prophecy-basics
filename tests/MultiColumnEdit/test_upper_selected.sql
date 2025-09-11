with actual as (
  {% set all_cols = ['id','country','first_name','age'] %}
  {{ MultiColumnEdit(
      relation=ref('people'),
      expressionToBeApplied='upper(column_name)',
      allColumnNames=all_cols,
      columnNames=['country','first_name'],
      changeOutputFieldName=false
  ) }}
),
expected as (
  select 1 as id, 'USA' as country, 'JOHN' as first_name, 50 as age
  union all select 2, 'INDIA', 'ANA', 40
  union all select 3, 'NORWAY', 'OLA', cast(null as int)
  union all select 4, 'UK', 'LI', 0
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff