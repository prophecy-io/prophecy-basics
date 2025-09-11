-- Comprehensive test for DynamicSelect with special characters and whitespace in column names
-- This test focuses on SELECT_TYPES mode with STRING columns that have special characters

WITH expected AS (
    SELECT 
        "user name",
        "user-email", 
        "data field"
    FROM {{ ref('special_chars_data') }}
),
actual AS (
    {{ DynamicSelect(
        ref('special_chars_data'),
        [
            {"name": "id", "dataType": "INTEGER"},
            {"name": "user name", "dataType": "STRING"},
            {"name": "user-email", "dataType": "STRING"},
            {"name": "data field", "dataType": "STRING"},
            {"name": "sales amount", "dataType": "DOUBLE"},
            {"name": "is active", "dataType": "BOOLEAN"},
            {"name": "created date", "dataType": "DATE"}
        ],
        ["STRING"],
        "SELECT_TYPES"
    ) }}
)
SELECT * FROM expected
EXCEPT
SELECT * FROM actual
UNION ALL
SELECT * FROM actual
EXCEPT  
SELECT * FROM expected
