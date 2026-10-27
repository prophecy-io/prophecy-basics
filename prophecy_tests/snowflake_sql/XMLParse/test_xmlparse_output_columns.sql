-- Test: XMLParse macro - Alteryx-style output columns (Snowflake)
-- With outputColumns, each entry becomes one string column read from the parsed document:
-- the root's value, attribute and outer XML, and each child's value, attribute and outer XML by
-- tag and occurrence. With childElement, every outermost element with that tag becomes its own row
-- and stands in for the root. Without outputColumns the macro keeps its single VARIANT column.
--
-- Values are compared in SQL, not through run_query results: agate would read '007' or '1.50'
-- as numbers and hide exactly the conversions this test guards against.

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY TABLE XML_PARSE_COLUMNS_SRC AS
SELECT * FROM (VALUES
    (1, '<order code="X"><id>007</id><amount>1.50</amount><item sku="a">A</item><item sku="b">B</item><note>x &amp; y</note><customer><name>N</name></customer><blank/></order>'),
    (2, '<order><id>2</id></order>'),
    (3, '<order><id>3</order>'),
    (4, NULL),
    (5, '<v>leaf</v>')
) AS T(ROW_ID, PAYLOAD)
{% endset %}
{% do run_query(create_src) %}

{% set cases = [] %}

-- every kind of output column; malformed XML and NULL give NULLs (ignoreErrors)
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['XML_PARSE_COLUMNS_SRC'],
    columnName='PAYLOAD',
    parsingMethod='',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "order", "kind": "root"}, {"name": "order_code", "kind": "root_attr", "attr": "code"}, {"name": "id", "kind": "child", "tag": "id", "instance": 0}, {"name": "amount", "kind": "child", "tag": "amount", "instance": 0}, {"name": "item", "kind": "child", "tag": "item", "instance": 0}, {"name": "item_2", "kind": "child", "tag": "item", "instance": 1}, {"name": "item_3", "kind": "child", "tag": "item", "instance": 2}, {"name": "item_2_sku", "kind": "child_attr", "tag": "item", "instance": 1, "attr": "sku"}, {"name": "note", "kind": "child", "tag": "note", "instance": 0}, {"name": "customer", "kind": "child", "tag": "customer", "instance": 0}, {"name": "blank", "kind": "child", "tag": "blank", "instance": 0}, {"name": "item_2_OuterXML", "kind": "child_outer", "tag": "item", "instance": 1}, {"name": "customer_OuterXML", "kind": "child_outer", "tag": "customer", "instance": 0}, {"name": "order_OuterXML", "kind": "root_outer"}],
    includeSourceColumn=true,
    ignoreErrors=true
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM (VALUES
    (1, '', 'X', '007', '1.50', 'A', 'B', NULL, 'b', 'x & y', '', '', '<item sku="b">B</item>', '<customer><name>N</name></customer>', '<order code="X"><id>007</id><amount>1.50</amount><item sku="a">A</item><item sku="b">B</item><note>x &amp; y</note><customer><name>N</name></customer><blank/></order>'),
    (2, '', NULL, '2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '<order><id>2</id></order>'),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '<order><id>3</order>'),
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (5, 'leaf', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '<v>leaf</v>')
) AS E(ROW_ID, "order", "order_code", "id", "amount", "item", "item_2", "item_3", "item_2_sku", "note", "customer", "blank", "item_2_OuterXML", "customer_OuterXML", "order_OuterXML")
{% endset %}
{% do cases.append({"name": "every kind of output column", "sql": macro_query, "expected": expected,
    "columns": ["ROW_ID", "PAYLOAD", "order", "order_code", "id", "amount", "item", "item_2", "item_3", "item_2_sku", "note", "customer", "blank", "item_2_OuterXML", "customer_OuterXML", "order_OuterXML"]}) %}

-- includeSourceColumn=false drops the XML column
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['XML_PARSE_COLUMNS_SRC'],
    columnName='PAYLOAD',
    parsingMethod='',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "id", "kind": "child", "tag": "id", "instance": 0}],
    includeSourceColumn=false,
    ignoreErrors=true
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM (VALUES (1, '007'), (2, '2'), (3, NULL), (4, NULL), (5, NULL)) AS E(ROW_ID, "id")
{% endset %}
{% do cases.append({"name": "includeSourceColumn=false drops the XML column", "sql": macro_query, "expected": expected,
    "columns": ["ROW_ID", "id"]}) %}

-- no outputColumns: the single VARIANT column, as before
{% set macro_query %}
SELECT ROW_ID, XMLGET("PAYLOAD_parsed", 'id'):"$"::VARCHAR AS "id" FROM ({{ prophecy_basics.XMLParse(
    relation_name=['XML_PARSE_COLUMNS_SRC'],
    columnName='PAYLOAD',
    parsingMethod='parseFromSampleRecord',
    sampleRecord='',
    sampleSchema=''
) }}) AS parsed WHERE ROW_ID IN (1, 2)
{% endset %}
{% set expected %}
SELECT * FROM (VALUES (1, '007'), (2, '2')) AS E(ROW_ID, "id")
{% endset %}
{% do cases.append({"name": "no outputColumns keeps the VARIANT column", "sql": macro_query, "expected": expected}) %}

{% set create_child_src %}
CREATE OR REPLACE TEMPORARY TABLE XML_PARSE_CHILD_SRC AS
SELECT * FROM (VALUES
    (1, '<doc><meta><issued>2024-01-01</issued></meta><list><row id="1"><v>a</v></row><row id="2"><v>b</v><row id="3"><v>c</v></row></row></list></doc>'),
    (2, '<doc><row id="9"><v>z</v></row></doc>'),
    (3, '<doc><none/></doc>'),
    (4, NULL),
    (5, '<doc><row id="5"></doc>')
) AS T(ROW_ID, PAYLOAD)
{% endset %}
{% do run_query(create_child_src) %}

-- childElement: one row per outermost <row> at any depth (row 3 sits inside row 2 and is not its own
-- row); the root kinds are the matched element; rows with no <row>, NULL or malformed XML are dropped
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['XML_PARSE_CHILD_SRC'],
    columnName='PAYLOAD',
    parsingMethod='',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "row", "kind": "root"}, {"name": "row_id", "kind": "root_attr", "attr": "id"}, {"name": "v", "kind": "child", "tag": "v", "instance": 0}, {"name": "row2", "kind": "child", "tag": "row", "instance": 0}, {"name": "row_OuterXML", "kind": "root_outer"}],
    includeSourceColumn=false,
    ignoreErrors=true,
    childElement='row'
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM (VALUES
    (1, '', '1', 'a', NULL, '<row id="1"><v>a</v></row>'),
    (1, '', '2', 'b', '', '<row id="2"><v>b</v><row id="3"><v>c</v></row></row>'),
    (2, '', '9', 'z', NULL, '<row id="9"><v>z</v></row>')
) AS E(ROW_ID, "row", "row_id", "v", "row2", "row_OuterXML")
{% endset %}
{% do cases.append({"name": "childElement gives one row per outermost occurrence", "sql": macro_query, "expected": expected,
    "columns": ["ROW_ID", "row", "row_id", "v", "row2", "row_OuterXML"]}) %}

-- childElement deeper than the root's children, holding a leaf value
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['XML_PARSE_CHILD_SRC'],
    columnName='PAYLOAD',
    parsingMethod='',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "issued", "kind": "root"}],
    includeSourceColumn=false,
    ignoreErrors=true,
    childElement='issued'
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM (VALUES (1, '2024-01-01')) AS E(ROW_ID, "issued")
{% endset %}
{% do cases.append({"name": "childElement finds a nested leaf element", "sql": macro_query, "expected": expected,
    "columns": ["ROW_ID", "issued"]}) %}

{% for case in cases %}
    {% if case.columns is defined %}
        {% set results = run_query(case.sql ~ " LIMIT 0") %}
        {% if (results.column_names | list) != case.columns %}
            {{ exceptions.raise_compiler_error("XMLParse test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
        {% endif %}
    {% endif %}
    {# rows in one result and not the other, as text so nothing is re-typed on the way back #}
    {% set diff_query %}
    WITH a AS ({{ case.sql }}), e AS ({{ case.expected }})
    SELECT LISTAGG(side || ' ' || TO_VARCHAR(r), '; ') FROM (
        SELECT 'got' AS side, OBJECT_CONSTRUCT_KEEP_NULL(*) AS r FROM (
            SELECT {% if case.columns is defined and 'PAYLOAD' in case.columns %}* EXCLUDE (PAYLOAD){% else %}*{% endif %} FROM a
            EXCEPT SELECT * FROM e)
        UNION ALL
        SELECT 'expected', OBJECT_CONSTRUCT_KEEP_NULL(*) FROM (
            SELECT * FROM e
            EXCEPT SELECT {% if case.columns is defined and 'PAYLOAD' in case.columns %}* EXCLUDE (PAYLOAD){% else %}*{% endif %} FROM a)
    )
    {% endset %}
    {% set mismatch = run_query(diff_query).columns[0].values()[0] %}
    {% if mismatch %}
        {{ exceptions.raise_compiler_error("XMLParse test '" ~ case.name ~ "' FAILED: " ~ mismatch) }}
    {% endif %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
