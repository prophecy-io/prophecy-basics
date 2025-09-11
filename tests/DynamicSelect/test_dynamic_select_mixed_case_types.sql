-- Test DynamicSelect with mixed case target types and data types
WITH expected AS (
    SELECT 
        "user_name",
        "user_email",
        "is_active"
    FROM {{ ref('sample_data') }}
),
actual AS (
    {{ DynamicSelect(
        ref('sample_data'),
        [
            {"name": "id", "dataType": "INTEGER"},
            {"name": "user_name", "dataType": "string"},
            {"name": "user_email", "dataType": "String"},
            {"name": "age", "dataType": "INTEGER"},
            {"name": "salary", "dataType": "DOUBLE"},
            {"name": "is_active", "dataType": "boolean"},
            {"name": "created_date", "dataType": "DATE"},
            {"name": "last_login", "dataType": "TIMESTAMP"}
        ],
        ["string", "Boolean"],
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
