-- Test DynamicSelect with case-insensitive data types
WITH expected AS (
    SELECT 
        "id",
        "age"
    FROM {{ ref('sample_data') }}
),
actual AS (
    {{ DynamicSelect(
        ref('sample_data'),
        [
            {"name": "id", "dataType": "integer"},
            {"name": "user_name", "dataType": "STRING"},
            {"name": "user_email", "dataType": "String"},
            {"name": "age", "dataType": "Integer"},
            {"name": "salary", "dataType": "DOUBLE"},
            {"name": "is_active", "dataType": "BOOLEAN"},
            {"name": "created_date", "dataType": "DATE"},
            {"name": "last_login", "dataType": "TIMESTAMP"}
        ],
        ["INTEGER"],
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
