-- Test: XMLParse macro - Alteryx-style output columns (Databricks)
-- With parsingMethod='parseToColumns', each outputColumns entry becomes one string column read from
-- the document with the xpath_* functions: the root's value, attribute and outer XML, and each
-- child's value, attribute and outer XML by tag and occurrence. With childElement, every <tag> becomes
-- its own row and stands in for the root. The legacy from_xml paths stay as they were.
--
-- Values are compared in Spark SQL (to_json of each row, EXCEPT both ways), not through run_query
-- results: agate would read '007' or '1.50' as numbers and hide the conversions this test guards.
-- On Databricks a childElement that nests inside itself fails the query on purpose; that case has no
-- row here (a dbt test cannot expect an error).

{% if execute %}
{% set create_src %}
CREATE OR REPLACE TEMPORARY VIEW xml_parse_columns_src AS
SELECT * FROM VALUES
    (1, '<order code="X"><id>007</id><amount>1.50</amount><item sku="a">A</item><item sku="b">B</item><note>  x &amp; y  </note><customer><name>N</name></customer><blank/></order>'),
    (2, '<order><id>2</id></order>'),
    (3, '<order><id>3</order>'),
    (4, CAST(NULL AS STRING)),
    (5, '<v>leaf</v>')
AS t(row_id, payload)
{% endset %}
{% do run_query(create_src) %}

{% set create_valid_src %}
CREATE OR REPLACE TEMPORARY VIEW xml_parse_valid_src AS
SELECT * FROM VALUES
    (1, '<order code="X"><item sku="a>b">A</item><item sku=\'b\'/><items>no</items></order>'),
    (2, '<order><id>2</id></order>'),
    (3, CAST(NULL AS STRING))
AS t(row_id, payload)
{% endset %}
{% do run_query(create_valid_src) %}

{% set create_child_src %}
CREATE OR REPLACE TEMPORARY VIEW xml_parse_child_src AS
SELECT * FROM VALUES
    (1, '<doc><meta><issued>2024-01-01</issued></meta><list><row id="1"><v>a</v></row><row id="2"><v>b</v><w/></row></list></doc>'),
    (2, '<doc><row id="9"><v>z</v></row></doc>'),
    (3, '<doc><none/></doc>'),
    (4, CAST(NULL AS STRING)),
    (5, '<doc><row id="5"></doc>')
AS t(row_id, payload)
{% endset %}
{% do run_query(create_child_src) %}

{% set cases = [] %}

-- every kind of output column; values trimmed as on Snowflake; malformed XML and NULL give NULLs (ignoreErrors)
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['xml_parse_columns_src'],
    columnName='payload',
    parsingMethod='parseToColumns',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "order", "kind": "root"}, {"name": "order_code", "kind": "root_attr", "attr": "code"}, {"name": "id", "kind": "child", "tag": "id", "instance": 0}, {"name": "amount", "kind": "child", "tag": "amount", "instance": 0}, {"name": "item", "kind": "child", "tag": "item", "instance": 0}, {"name": "item_2", "kind": "child", "tag": "item", "instance": 1}, {"name": "item_3", "kind": "child", "tag": "item", "instance": 2}, {"name": "item_2_sku", "kind": "child_attr", "tag": "item", "instance": 1, "attr": "sku"}, {"name": "note", "kind": "child", "tag": "note", "instance": 0}, {"name": "customer", "kind": "child", "tag": "customer", "instance": 0}, {"name": "blank", "kind": "child", "tag": "blank", "instance": 0}, {"name": "item_2_OuterXML", "kind": "child_outer", "tag": "item", "instance": 1}, {"name": "customer_OuterXML", "kind": "child_outer", "tag": "customer", "instance": 0}, {"name": "order_OuterXML", "kind": "root_outer"}],
    includeSourceColumn=true,
    ignoreErrors=true
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM VALUES
    (1, '', 'X', '007', '1.50', 'A', 'B', CAST(NULL AS STRING), 'b', 'x & y', '', '', '<item sku="b">B</item>', '<customer><name>N</name></customer>', '<order code="X"><id>007</id><amount>1.50</amount><item sku="a">A</item><item sku="b">B</item><note>  x &amp; y  </note><customer><name>N</name></customer><blank/></order>'),
    (2, '', NULL, '2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '<order><id>2</id></order>'),
    (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '<order><id>3</order>'),
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
    (5, 'leaf', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '<v>leaf</v>')
AS e(row_id, `order`, order_code, id, amount, item, item_2, item_3, item_2_sku, note, customer, blank, item_2_OuterXML, customer_OuterXML, order_OuterXML)
{% endset %}
{% do cases.append({"name": "every kind of output column", "sql": macro_query, "expected": expected, "drop": "payload",
    "columns": ["row_id", "payload", "order", "order_code", "id", "amount", "item", "item_2", "item_3", "item_2_sku", "note", "customer", "blank", "item_2_OuterXML", "customer_OuterXML", "order_OuterXML"]}) %}

-- includeSourceColumn=false drops the XML column
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['xml_parse_columns_src'],
    columnName='payload',
    parsingMethod='parseToColumns',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "id", "kind": "child", "tag": "id", "instance": 0}],
    includeSourceColumn=false,
    ignoreErrors=true
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM VALUES (1, '007'), (2, '2'), (3, CAST(NULL AS STRING)), (4, NULL), (5, NULL) AS e(row_id, id)
{% endset %}
{% do cases.append({"name": "includeSourceColumn=false drops the XML column", "sql": macro_query, "expected": expected,
    "columns": ["row_id", "id"]}) %}

-- ignoreErrors=false (valid documents only, or the query fails); outer XML of elements whose attributes
-- hold '>' or single quotes, a self-closing one, and a longer tag starting the same; a quoted name
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['xml_parse_valid_src'],
    columnName='payload',
    parsingMethod='parseToColumns',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "item `a`", "kind": "child", "tag": "item", "instance": 0}, {"name": "item_OuterXML", "kind": "child_outer", "tag": "item", "instance": 0}, {"name": "item_2_OuterXML", "kind": "child_outer", "tag": "item", "instance": 1}, {"name": "code", "kind": "root_attr", "attr": "code"}],
    includeSourceColumn=false,
    ignoreErrors=false
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM VALUES
    (1, 'A', '<item sku="a>b">A</item>', '<item sku=\'b\'/>', 'X'),
    (2, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING)),
    (3, NULL, NULL, NULL, NULL)
AS e(row_id, `item ``a```, item_OuterXML, item_2_OuterXML, code)
{% endset %}
{% do cases.append({"name": "ignoreErrors=false, outer XML of awkward elements, a quoted name", "sql": macro_query, "expected": expected,
    "columns": ["row_id", "item `a`", "item_OuterXML", "item_2_OuterXML", "code"]}) %}

-- childElement: one row per <row> at any depth, the root kinds read from it; rows with no <row>, NULL
-- or malformed XML are dropped
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['xml_parse_child_src'],
    columnName='payload',
    parsingMethod='parseToColumns',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "row", "kind": "root"}, {"name": "row_attr_id", "kind": "root_attr", "attr": "id"}, {"name": "v", "kind": "child", "tag": "v", "instance": 0}, {"name": "w_OuterXML", "kind": "child_outer", "tag": "w", "instance": 0}, {"name": "row_OuterXML", "kind": "root_outer"}],
    includeSourceColumn=false,
    ignoreErrors=true,
    childElement='row'
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM VALUES
    (1, '', '1', 'a', CAST(NULL AS STRING), '<row id="1"><v>a</v></row>'),
    (1, '', '2', 'b', '<w/>', '<row id="2"><v>b</v><w/></row>'),
    (2, '', '9', 'z', NULL, '<row id="9"><v>z</v></row>')
AS e(row_id, `row`, row_attr_id, v, w_OuterXML, row_OuterXML)
{% endset %}
{% do cases.append({"name": "childElement gives one row per occurrence", "sql": macro_query, "expected": expected,
    "columns": ["row_id", "row", "row_attr_id", "v", "w_OuterXML", "row_OuterXML"]}) %}

-- childElement deeper than the root's children, holding a leaf value
{% set macro_query %}
SELECT * FROM ({{ prophecy_basics.XMLParse(
    relation_name=['xml_parse_child_src'],
    columnName='payload',
    parsingMethod='parseToColumns',
    sampleRecord='',
    sampleSchema='',
    outputColumns=[{"name": "issued", "kind": "root"}],
    includeSourceColumn=false,
    ignoreErrors=true,
    childElement='issued'
) }}) AS parsed
{% endset %}
{% set expected %}
SELECT * FROM VALUES (1, '2024-01-01') AS e(row_id, issued)
{% endset %}
{% do cases.append({"name": "childElement finds a nested leaf element", "sql": macro_query, "expected": expected,
    "columns": ["row_id", "issued"]}) %}

-- the legacy sample-record path: a sample holding a quote still compiles and parses
{% set macro_query %}
SELECT row_id, payload_parsed.id AS id FROM ({{ prophecy_basics.XMLParse(
    relation_name=['xml_parse_valid_src'],
    columnName='payload',
    parsingMethod='parseFromSampleRecord',
    sampleRecord='<order note="it\'s"><id>a</id></order>',
    sampleSchema=''
) }}) AS parsed WHERE row_id = 2
{% endset %}
{% set expected %}
SELECT * FROM VALUES (2, '2') AS e(row_id, id)
{% endset %}
{% do cases.append({"name": "legacy sample record with a quote", "sql": macro_query, "expected": expected}) %}

{% for case in cases %}
    {% if case.columns is defined %}
        {% set results = run_query(case.sql ~ " LIMIT 0") %}
        {% if (results.column_names | list) != case.columns %}
            {{ exceptions.raise_compiler_error("XMLParse test '" ~ case.name ~ "' FAILED: expected columns " ~ case.columns ~ ", got " ~ (results.column_names | list)) }}
        {% endif %}
    {% endif %}
    {# rows in one result and not the other, as JSON text so nothing is re-typed on the way back #}
    {% set got %}SELECT {{ '* except (' ~ case.drop ~ ')' if case.drop is defined else '*' }} FROM a{% endset %}
    {% set diff_query %}
    WITH a AS ({{ case.sql }}), e AS ({{ case.expected }})
    SELECT concat_ws('; ', collect_list(side || ' ' || r)) FROM (
        SELECT 'got' AS side, to_json(struct(*), map('ignoreNullFields', 'false')) AS r FROM ({{ got }} EXCEPT SELECT * FROM e)
        UNION ALL
        SELECT 'expected', to_json(struct(*), map('ignoreNullFields', 'false')) FROM (SELECT * FROM e EXCEPT {{ got }})
    )
    {% endset %}
    {% set mismatch = run_query(diff_query).columns[0].values()[0] %}
    {% if mismatch %}
        {{ exceptions.raise_compiler_error("XMLParse test '" ~ case.name ~ "' FAILED: " ~ mismatch) }}
    {% endif %}
{% endfor %}

{% endif %}

SELECT 1 WHERE 1=0
