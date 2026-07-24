WITH raw_data AS (

  SELECT
    'John Doe' AS name,
    30 AS age,
    'Engineer' AS profession,
    75000.0 AS salary,
    TRUE AS is_active,
    DATE('2025-04-15') AS join_date,
    1.75 AS height_meters,
    'New York' AS city,
    'USA' AS country,
    current_timestamp() AS query_timestamp

),

pass_through AS (

  SELECT *

  FROM raw_data

),

find_duplicates_1 AS (

  {{
    prophecy_basics.FindDuplicates(
      ['pass_through'],
      [],
      'equal_to',
      'custom_group_count',
      '4',
      '',
      '',
      'allCols',
      [
        'name',
        'age',
        'profession',
        'salary',
        'is_active',
        'join_date',
        'height_meters',
        'city',
        'country',
        'query_timestamp'
      ],
      []
    )
  }}

),

replace_nulls AS (

  {{
    prophecy_basics.DataCleansing(
      ['find_duplicates_1'],
      [
        { "name": "name", "dataType": "String" },
        { "name": "age", "dataType": "Integer" },
        { "name": "profession", "dataType": "String" },
        { "name": "salary", "dataType": "Decimal(6, 1)" },
        { "name": "is_active", "dataType": "Boolean" },
        { "name": "join_date", "dataType": "Date" },
        { "name": "height_meters", "dataType": "Decimal(3, 2)" },
        { "name": "city", "dataType": "String" },
        { "name": "country", "dataType": "String" },
        { "name": "query_timestamp", "dataType": "Timestamp" }
      ],
      'keepOriginal',
      [
        'name',
        'age',
        'profession',
        'salary',
        'is_active',
        'join_date',
        'height_meters',
        'city',
        'country',
        'query_timestamp'
      ],
      true,
      'NA',
      true,
      0,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      '1970-01-01',
      true,
      '1970-01-01 00:00:00.0'
    )
  }}

),

Union_1 AS (

  SELECT *

  FROM replace_nulls AS in0

  UNION ALL

  SELECT *

  FROM pass_through AS in1

),

sample_first50 AS (

  {{
    prophecy_basics.Sample(
      ['Union_1'],
      '[{"name": "name", "dataType": "String"}, {"name": "age", "dataType": "Integer"}, {"name": "profession", "dataType": "String"}, {"name": "salary", "dataType": "Decimal(6, 1)"}, {"name": "is_active", "dataType": "Boolean"}, {"name": "join_date", "dataType": "Date"}, {"name": "height_meters", "dataType": "Decimal(3, 2)"}, {"name": "city", "dataType": "String"}, {"name": "country", "dataType": "String"}, {"name": "query_timestamp", "dataType": "Timestamp"}]',
      'sampleDataset',
      [],
      1002,
      'firstN',
      50,
      [
        { 'expression': { 'expression': 'age' }, 'sortType': 'asc' },
        { 'expression': { 'expression': 'salary' }, 'sortType': 'asc' }
      ]
    )
  }}

),

RecordID_1 AS (

  {{
    prophecy_basics.RecordID(
      ['sample_first50'],
      'incremental_id',
      'RecordID',
      'string',
      6,
      1000,
      'tableLevel',
      'first_column',
      [],
      []
    )
  }}

)

SELECT *

FROM RecordID_1
