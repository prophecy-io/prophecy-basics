{{
  config({
    "materialized": "ephemeral"
  })
}}

WITH upstream_cte AS (

  SELECT 1 AS id

),

todo_with_inputs AS (

  {{ prophecy_basics.ToDo("Component type: Report Text isn't \"supported\".", ['upstream_cte'], "Report Text has no SQL form", "<Node ToolID=\"29\">\n  <GuiSettings Plugin=\"PortfolioComposerText\"/>\n  <Value name=\"Text\">it's \"quoted\" & <b>bold</b>, é漢字</Value>\n</Node>") }}

)

SELECT *

FROM todo_with_inputs
