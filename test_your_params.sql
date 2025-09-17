{{
  config({    
    "materialized": "ephemeral",
    "database": "database",
    "schema": "schema"
  })
}}

WITH text_to_column_csv_1 AS (

  SELECT *
  
  FROM text_to_column_csv_1

),

split_text_columns AS (

  {{
    prophecy_basics.TextToColumns(
      'text_to_column_csv_1', 
      'ADDRESS', 
      "\\\|", 
      'splitColumns', 
      5, 
      'Leave extra in last column', 
      'root', 
      'generated', 
      'generated_column'
    )
  }}

)

SELECT *

FROM split_text_columns
