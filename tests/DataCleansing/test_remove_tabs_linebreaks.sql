with actual as (
    {{ DataCleansing(
        ref('raw_data_test'),
        [
            { "name": "id", "dataType": "int" },
            { "name": "name", "dataType": "string" },
            { "name": "age", "dataType": "int" },
            { "name": "bio", "dataType": "string" },
            { "name": "joined_on", "dataType": "date" },
            { "name": "last_seen_at", "dataType": "timestamp" }
        ],
        columnNames=['name','bio'],
        removeTabsLineBreaksAndDuplicateWhitespace=True
    ) }}
),
expected as (
    select 1, 'Alice', 30, 'Hello, World!', cast('2020-05-01' as date), cast('2023-05-01 12:34:56' as timestamp)
    union all
    select 2, NULL, 25, 'Line1 Line2', cast('2021-03-15' as date), cast('2023-06-10 08:00:00' as timestamp)
    union all
    select 3, ' Bob ', 40, 'Tabs and spaces', NULL, cast('2020-01-01 00:00:00' as timestamp)
    union all
    select 4, NULL, NULL, NULL, NULL, NULL
    union all
    select 5, 'Elodie', 28, ' Punctuations!!!', cast('2022-12-31' as date), NULL
)
, diff as (
    (select * from actual except select * from expected)
    union all
    (select * from expected except select * from actual)
)
select * from diff
