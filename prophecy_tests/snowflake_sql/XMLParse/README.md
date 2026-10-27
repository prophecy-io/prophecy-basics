# XMLParse Macro Tests (Snowflake)

dbt tests for the `XMLParse` macro (`macros/XMLParse.sql`) on Snowflake, through the
`snowflake__` adapter.

## Test Files

1. **`test_xmlparse_output_columns.sql`** - Alteryx-style output columns (`outputColumns`)
- root value, root attribute and root outer XML
- child values by tag and occurrence (`item`, `item_2`), a missing occurrence (NULL)
- child attribute and child outer XML
- a child with element children and an empty child (`''`), an entity (`&amp;`)
- numeric text kept as written (`007`, `1.50`)
- malformed XML and NULL input give NULLs (`ignoreErrors`)
- the source column kept, and dropped with `includeSourceColumn=false`
- no `outputColumns`: the single VARIANT `<column>_parsed` column, as before

## How These Tests Work

Each test:
1. Creates a temporary source table
2. Calls the `XMLParse` macro once per case
3. Checks the column list, then diffs the rows against the expected rows in SQL
   (`EXCEPT` both ways), so text such as `1.50` is never re-typed by agate on the way back
4. Raises a compiler error naming the case and the differing rows
