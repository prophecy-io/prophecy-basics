with actual as (
    {{ DataCleansing(
        ref('raw_data_all_whitespace'),
        [
            { "name": "id", "dataType": "int" },
            { "name": "name", "dataType": "string" },
            { "name": "age", "dataType": "int" },
            { "name": "bio", "dataType": "string" },
            { "name": "joined_on", "dataType": "date" },
            { "name": "last_seen_at", "dataType": "timestamp" }
        ],
        columnNames=['bio'],
        allWhiteSpace=True
    ) }}
),
expected as (
    select 1, 'Alice', 30, 'Hello', cast('2020-05-01' as date), cast('2023-05-01 12:34:56' as timestamp)
    union all
    select 2, 'Bob', 40, 'LineBreaks', NULL, cast('2020-01-01 00:00:00' as timestamp)
),
diff as (
    (select * from actual except select * from expected)
    union all
    (select * from expected except select * from actual)
)
select * from diff
