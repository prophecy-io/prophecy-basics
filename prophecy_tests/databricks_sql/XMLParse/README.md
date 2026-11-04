# XMLParse Macro Tests (Databricks)

dbt tests for the `XMLParse` macro (`macros/XMLParse.sql`) on Databricks, through the `default__`
adapter, with `parsingMethod='parseToColumns'`.

## Test Files

1. **`test_xmlparse_output_columns.sql`** - Alteryx-style output columns (`outputColumns`)
- root value, root attribute and root outer XML; values trimmed as on Snowflake
- child values by tag and occurrence (`item`, `item_2`), a missing occurrence (NULL)
- child attribute and child outer XML, cut out of the document as written: attributes holding `>`
  or single quotes, a self-closing element, and a longer tag starting the same (`<items>`)
- a child with element children and an empty child (`''`), an entity (`&amp;`)
- numeric text kept as written (`007`, `1.50`)
- malformed XML and NULL input give NULLs (`ignoreErrors`); `ignoreErrors=false` on valid documents
- the source column kept, and dropped with `includeSourceColumn=false`; an output name with a backtick
- `childElement`: one row per occurrence at any depth, the root kinds read from it, and input rows
  with no match, NULL or malformed XML dropped; a nested leaf element as the parsed element
- the legacy sample-record path with a quote in the sample

A `childElement` that nests inside itself, appears in a comment or CDATA, or is the root makes the
query fail with `raise_error` on Databricks (dbt tests cannot expect an error, so no case here).

## How These Tests Work

Each test:
1. Creates a temporary source view
2. Calls the `XMLParse` macro once per case
3. Checks the column list, then diffs the rows against the expected rows in Spark SQL
   (`to_json` of each row, `EXCEPT` both ways), so text such as `1.50` is never re-typed by agate
4. Raises a compiler error naming the case and the differing rows
