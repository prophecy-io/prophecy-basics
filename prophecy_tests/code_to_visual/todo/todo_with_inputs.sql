{{
  config({
    "materialized": "ephemeral"
  })
}}

WITH upstream_cte AS (

  SELECT 1 AS id

),

todo_with_inputs AS (

  {{ prophecy_basics.ToDo("Component type: Report Text isn't \"supported\".", ['upstream_cte']) }}

)

SELECT *

FROM todo_with_inputs
