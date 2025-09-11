-- Test DynamicSelect with special characters using SELECT_EXPR mode
-- Tests expression-based selection with columns containing special characters

WITH expected AS (
    SELECT 
        "user name",
        "user-email"
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
        [],
        "SELECT_EXPR",
        "column_name LIKE '%user%'"
    ) }}
)
SELECT * FROM expected
EXCEPT
SELECT * FROM actual
UNION ALL
SELECT * FROM actual
EXCEPT  
SELECT * FROM expected
