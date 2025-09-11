-- Remove rows only (drop all-null rows), no replacements; title-case name & bio
with actual as (

    {{ DataCleansing(
        relation_name=ref('people_raw'),
        schema=[
            {"name": "id", "dataType": "integer"},
            {"name": "name", "dataType": "string"},
            {"name": "age", "dataType": "integer"},
            {"name": "bio", "dataType": "string"},
            {"name": "joined_on", "dataType": "date"},
            {"name": "last_seen_at", "dataType": "timestamp"}
        ],
        modifyCase='makeTitlecase',
        columnNames=['name','bio'],
        replaceNullTextFields=False,
        replaceNullForNumericFields=False,
        replaceNullDateFields=False,
        replaceNullTimeFields=False,
        removeRowNullAllCols=True
    ) }}

), expected as (

    select 1 as id, 'Alice' as name, 30 as age, 'Hello, World!' as bio, DATE '2020-05-01' as joined_on, TIMESTAMP '2023-05-01 12:34:56' as last_seen_at
    union all
    select 2, null, 25, 'Line1 Line2', DATE '2021-03-15', TIMESTAMP '2023-06-10 08:00:00'
    union all
    select 3, 'Bob', 40, 'Tabs And Spaces', null, TIMESTAMP '2020-01-01 00:00:00'
    union all
    select 4, null, null, null, null, null
    union all
    select 5, 'Elodie', 28, 'Punctuations', DATE '2022-12-31', null

), diff as (
    (select * from actual except select * from expected)
    union all
    (select * from expected except select * from actual)
)
select * from diff
