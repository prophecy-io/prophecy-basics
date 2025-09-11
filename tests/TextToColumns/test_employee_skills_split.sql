-- Test Case: Split employee skills into individual rows
-- Functional scenario: HR analysis - skill inventory and matching
with actual as (
  {{ TextToColumns(
      relation_name=ref('employee_skills'),
      columnName='skills',
      delimiter=',',
      split_strategy='splitRows',
      noOfColumns=0,
      leaveExtraCharLastCol=false,
      splitColumnPrefix='unused',
      splitColumnSuffix='unused',
      splitRowsColumnName='skill'
  ) }}
),
expected as (
  select 1 as id, 'John Doe' as employee_name, 'Python,SQL,Docker,AWS' as skills, 'Python' as skill union all
  select 1 as id, 'John Doe' as employee_name, 'Python,SQL,Docker,AWS' as skills, 'SQL' as skill union all
  select 1 as id, 'John Doe' as employee_name, 'Python,SQL,Docker,AWS' as skills, 'Docker' as skill union all
  select 1 as id, 'John Doe' as employee_name, 'Python,SQL,Docker,AWS' as skills, 'AWS' as skill union all
  select 2 as id, 'Jane Smith' as employee_name, 'JavaScript,React,Node.js' as skills, 'JavaScript' as skill union all
  select 2 as id, 'Jane Smith' as employee_name, 'JavaScript,React,Node.js' as skills, 'React' as skill union all
  select 2 as id, 'Jane Smith' as employee_name, 'JavaScript,React,Node.js' as skills, 'Node.js' as skill union all
  select 3 as id, 'Bob Wilson' as employee_name, 'Java,Spring,Kubernetes,PostgreSQL,Redis' as skills, 'Java' as skill union all
  select 3 as id, 'Bob Wilson' as employee_name, 'Java,Spring,Kubernetes,PostgreSQL,Redis' as skills, 'Spring' as skill union all
  select 3 as id, 'Bob Wilson' as employee_name, 'Java,Spring,Kubernetes,PostgreSQL,Redis' as skills, 'Kubernetes' as skill union all
  select 3 as id, 'Bob Wilson' as employee_name, 'Java,Spring,Kubernetes,PostgreSQL,Redis' as skills, 'PostgreSQL' as skill union all
  select 3 as id, 'Bob Wilson' as employee_name, 'Java,Spring,Kubernetes,PostgreSQL,Redis' as skills, 'Redis' as skill union all
  select 4 as id, 'Alice Brown' as employee_name, NULL as skills, '' as skill union all
  select 5 as id, 'Tom Green' as employee_name, NULL as skills, '' as skill
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
