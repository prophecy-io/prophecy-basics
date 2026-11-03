{{
  config({
    "materialized": "ephemeral"
  })
}}

WITH upstream_cte AS (

  SELECT 1 AS id

),

legacy_todo AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

)

SELECT *

FROM legacy_todo
