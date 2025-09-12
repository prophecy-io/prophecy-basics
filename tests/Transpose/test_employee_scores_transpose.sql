-- Test Case: Transpose employee scores with single key column
-- Functional scenario: Employee performance analysis across subjects
with actual as (
  {{ prophecy_basics.Transpose(
      relation_name=ref('employee_scores'),
      keyColumns=['employee_id'],
      dataColumns=['math_score', 'english_score', 'science_score', 'history_score'],
      nameColumn='subject',
      valueColumn='score'
  ) }}
),
expected as (
  select 101 as employee_id, 'math_score' as subject, '85' as score union all
  select 101 as employee_id, 'english_score' as subject, '78' as score union all
  select 101 as employee_id, 'science_score' as subject, '92' as score union all
  select 101 as employee_id, 'history_score' as subject, '88' as score union all
  select 102 as employee_id, 'math_score' as subject, '72' as score union all
  select 102 as employee_id, 'english_score' as subject, '89' as score union all
  select 102 as employee_id, 'science_score' as subject, '76' as score union all
  select 102 as employee_id, 'history_score' as subject, '82' as score union all
  select 103 as employee_id, 'math_score' as subject, '68' as score union all
  select 103 as employee_id, 'english_score' as subject, '75' as score union all
  select 103 as employee_id, 'science_score' as subject, '71' as score union all
  select 103 as employee_id, 'history_score' as subject, '79' as score union all
  select 104 as employee_id, 'math_score' as subject, '90' as score union all
  select 104 as employee_id, 'english_score' as subject, '85' as score union all
  select 104 as employee_id, 'science_score' as subject, '88' as score union all
  select 104 as employee_id, 'history_score' as subject, '91' as score union all
  select 105 as employee_id, 'math_score' as subject, '88' as score union all
  select 105 as employee_id, 'english_score' as subject, '82' as score union all
  select 105 as employee_id, 'science_score' as subject, '85' as score union all
  select 105 as employee_id, 'history_score' as subject, '87' as score
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
