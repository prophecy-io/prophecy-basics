with actual as (
  {% set all_cols = ['id','country','first_name','age'] %}
  {{ MultiColumnEdit(
      relation=ref('people'),
      expressionToBeApplied='upper(column_name)',
      allColumnNames=all_cols,
      columnNames=['country','first_name'],
      changeOutputFieldName=true,
      prefixSuffixOption='prefix',
      prefixSuffixToBeAdded='p_'
  ) }}
),
-- Column order produced by your macro in this mode:
-- [all original columns...] then computed in columnNames order: p_country, p_first_name
expected as (
  select 1 as id, 'usa' as country, 'John' as first_name, 50 as age, 'USA' as p_country, 'JOHN' as p_first_name
  union all select 2, 'India', 'Ana', 40, 'INDIA', 'ANA'
  union all select 3, 'norway', 'Ola', cast(null as int), 'NORWAY', 'OLA'
  union all select 4, 'UK', 'li', 0, 'UK', 'LI'
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff