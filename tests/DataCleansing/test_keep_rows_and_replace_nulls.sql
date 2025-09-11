-- Keep rows and replace nulls; title-case name & bio; collapse whitespace/trim
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
        columnNames=['name','bio','age','joined_on','last_seen_at'],
        replaceNullTextFields=True,
        replaceNullTextWith='NA',
        replaceNullForNumericFields=True,
        replaceNullNumericWith=0,
        replaceNullDateFields=True,
        replaceNullDateWith='1970-01-01',
        replaceNullTimeFields=True,
        replaceNullTimeWith='1970-01-01 00:00:00',
        trimWhiteSpace=True,
        removeTabsLineBreaksAndDuplicateWhitespace=True,
        removeRowNullAllCols=False
    ) }}

), expected as (

    select 1 as id, 'Alice' as name, 30 as age, 'Hello, World!' as bio, DATE '2020-05-01' as joined_on, TIMESTAMP '2023-05-01 12:34:56' as last_seen_at
    union all
    -- name was NULL -> COALESCE('NA') -> titlecase -> 'Na'
    select 2, 'Na', 25, 'Line1 Line2', DATE '2021-03-15', TIMESTAMP '2023-06-10 08:00:00'
    union all
    -- joined_on NULL -> replaced with 1970-01-01
    select 3, 'Bob', 40, 'Tabs And Spaces', DATE '1970-01-01', TIMESTAMP '2020-01-01 00:00:00'
    union all
    -- fully-null row: name->'Na', age->0, bio->'Na', dates/times -> defaults
    select 4, 'Na', 0, 'Na', DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00'
    union all
    select 5, 'Elodie', 28, 'Punctuations', DATE '2022-12-31', TIMESTAMP '1970-01-01 00:00:00'

), diff as (
    (select * from actual except select * from expected)
    union all
    (select * from expected except select * from actual)
)
select * from diff
