-- Test Case: Extract just timestamp from log entries (noOfColumns=1 scenario)
-- Functional scenario: Log analysis - extracting timestamps for time-series analysis
with actual as (
  {{ TextToColumns(
      relation_name=ref('log_entries'),
      columnName='log_line',
      delimiter='|',
      split_strategy='splitColumns',
      noOfColumns=1,
      leaveExtraCharLastCol=true,
      splitColumnPrefix='log',
      splitColumnSuffix='data',
      splitRowsColumnName='unused'
  ) }}
),
expected as (
  select 1 as id, '2024-01-15 10:30:45|ERROR|Database connection failed' as log_line,
         '2024-01-15 10:30:45|ERROR|Database connection failed' as log_1_data union all
  select 2 as id, '2024-01-15 10:31:02|INFO|User login successful' as log_line,
         '2024-01-15 10:31:02|INFO|User login successful' as log_1_data union all
  select 3 as id, '2024-01-15 10:31:15|WARN|Memory usage high' as log_line,
         '2024-01-15 10:31:15|WARN|Memory usage high' as log_1_data
)
select *
from (
  (select * from actual except all select * from expected)
  union all
  (select * from expected except all select * from actual)
) diff
