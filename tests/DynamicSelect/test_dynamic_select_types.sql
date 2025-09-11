-- Test DynamicSelect with SELECT_TYPES mode
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
            {"name": "id", "dataType": "INTEGER"},
            {"name": "user_name", "dataType": "STRING"},
            {"name": "user_email", "dataType": "STRING"},
            {"name": "age", "dataType": "INTEGER"},
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
