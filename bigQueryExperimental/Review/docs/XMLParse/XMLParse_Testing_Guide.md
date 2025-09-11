# XMLParse Testing Guide

## Overview

This guide provides comprehensive testing scenarios for the XMLParse gem (bq_XMLParse.sql), which uses BigQuery native functions to parse XML strings and extract structured data. The gem generates dynamic SQL based on sample records or schemas and returns all original columns plus a structured XML column.

## Available Test Columns

The test data contains the following XML columns:
- `simple_xml` - Basic XML structures (products, persons, books)
- `complex_xml` - Complex nested XML with multiple levels
- `nested_xml` - Deeply nested XML structures
- `malformed_xml` - Intentionally malformed XML for error testing
- `xml_with_attributes` - XML with element attributes
- `xml_with_namespaces` - XML with namespace declarations
- `xml_with_cdata` - XML with CDATA sections
- `xml_with_comments` - XML with comments
- `xml_with_special_chars` - XML with special characters and Unicode
- `xml_with_entities` - XML with entity references

## Key Principle: Structure Matching

**The output structure should match the provided input structure:**
- **Sample Record**: If you provide a nested XML sample record, the output should preserve that nested structure
- **Sample Schema**: If you provide a nested STRUCT schema, the output should preserve that nested structure
- **Flattened Input**: If you provide a flat XML sample record, the output should be flat
- **Flat Schema**: If you provide a flat STRUCT schema, the output should be flat

This ensures that the parsing method respects the intended data structure as specified by the user.

## Test Scenarios

### Test 1: Empty Column Name (Error Handling)
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: (empty)
- **Parsing Method**: parseFromSchema
- **Sample Record**: (empty)
- **Sample Schema**: STRUCT<name: STRING, age: INT>

**Purpose**: Test behavior with empty column name
**Expected Result**: 
- Return all original data without parsing (fallback to SELECT *)
- No new columns added
**Validation**: Verify graceful handling of empty column name

## Basic Functionality Tests

### Test 2: simple_xml - Sample Record Approach
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: simple_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<product>
  <name>Widget Pro</name>
  <price>29.99</price>
  <category>Tools</category>
</product>
```
- **Sample Schema**: (empty)

**Purpose**: Test basic product data parsing with sample record
**Expected Result**: 
- All original columns preserved
- New column: `simple_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `name` (STRING), `price` (STRING), `category` (STRING)
**Validation**: Verify basic field extraction and type detection

### Test 3: simple_xml - Sample Schema Approach
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: simple_xml
- **Parsing Method**: parseFromSchema
- **Sample Record**: (empty)
- **Sample Schema**: STRUCT<name: STRING, price: FLOAT, category: STRING>

**Purpose**: Test basic product data parsing with schema
**Expected Result**: 
- All original columns preserved
- New column: `simple_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `name` (STRING), `price` (FLOAT64), `category` (STRING)
**Validation**: Verify schema-based field extraction

### Test 4: complex_xml - Sample Record Approach
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: complex_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<root>
  <user id="456" role="admin">
    <name>John Doe</name>
    <address>
      <street>123 Main St</street>
      <city>New York</city>
    </address>
  </user>
  <orders>
    <order id="1" amount="99.99"/>
    <order id="2" amount="149.99"/>
  </orders>
</root>
```
- **Sample Schema**: (empty)

**Purpose**: Test complex nested XML parsing with sample record
**Expected Result**: 
- All original columns preserved
- New column: `complex_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `user` (STRUCT with `@attributes`, `name`, `address`), `orders` (XML_EXTRACT for array)
**Validation**: Verify nested object handling and array preservation

### Test 5: complex_xml - Sample Schema Approach
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: complex_xml
- **Parsing Method**: parseFromSchema
- **Sample Record**: (empty)
- **Sample Schema**: STRUCT<user: STRUCT<id: STRING, name: STRING, address: STRUCT<street: STRING, city: STRING>>, orders: ARRAY<STRUCT<id: STRING, amount: FLOAT>>>

**Purpose**: Test complex nested XML parsing with schema
**Expected Result**: 
- All original columns preserved
- New column: `complex_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `user` (STRUCT containing `id`, `name`, `address`), `address` (STRUCT containing `street`, `city`), `orders` (XML_EXTRACT for array)
- **Structure**: Nested STRUCT matching the provided schema structure
**Validation**: Verify schema-based nested object handling preserves hierarchy

### Test 6: complex_xml - Sample Record Approach (Array-like Structure)
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: complex_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<catalog>
  <category>Electronics</category>
  <products>
    <product>
      <id>701</id>
      <name>Smartphone</name>
      <brand>TechCorp</brand>
      <price>699.99</price>
      <specs>
        <screen>6.1 inch</screen>
        <camera>48MP</camera>
        <storage>128GB</storage>
        <ram>8GB</ram>
      </specs>
      <reviews>
        <review>
          <rating>5</rating>
          <comment>Great phone!</comment>
          <user>John</user>
        </review>
        <review>
          <rating>4</rating>
          <comment>Good value</comment>
          <user>Jane</user>
        </review>
      </reviews>
    </product>
  </products>
</catalog>
```
- **Sample Schema**: (empty)

**Purpose**: Test XML array parsing with sample record
**Expected Result**: 
- All original columns preserved
- New column: `complex_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `item` (XML_EXTRACT for array - preserved as array)
**Validation**: Verify XML array handling

### Test 7: complex_xml - Sample Schema Approach (Array-like Structure)
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: complex_xml
- **Parsing Method**: parseFromSchema
- **Sample Record**: (empty)
- **Sample Schema**: ARRAY<STRUCT<id: STRING, name: STRING, price: FLOAT>>

**Purpose**: Test XML array parsing with schema
**Expected Result**: 
- All original columns preserved
- New column: `complex_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `item` (XML_EXTRACT for array - preserved as array)
**Validation**: Verify schema-based array handling

### Test 8: simple_xml - Sample Record Approach
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: simple_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<status>active</status>
<score>85</score>
```
- **Sample Schema**: (empty)

**Purpose**: Test simple XML parsing with sample record
**Expected Result**: 
- All original columns preserved
- New column: `simple_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `status` (STRING), `score` (STRING)
**Validation**: Verify simple field extraction and type detection

### Test 9: simple_xml - Sample Schema Approach
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: simple_xml
- **Parsing Method**: parseFromSchema
- **Sample Record**: (empty)
- **Sample Schema**: STRUCT<status: STRING, score: INT>

**Purpose**: Test simple XML parsing with schema
**Expected Result**: 
- All original columns preserved
- New column: `simple_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `status` (STRING), `score` (INT64)
**Validation**: Verify schema-based simple field extraction

### Test 10: invalid_xml - Sample Record Approach
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: invalid_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<person id="123">
  <name>Test</name>
  <age>25</age>
  <email>test@example.com</email>
</person>
```
- **Sample Schema**: (empty)

**Purpose**: Test invalid XML handling with sample record
**Expected Result**: 
- All original columns preserved
- New column: `invalid_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `@attributes` (STRUCT with id), `name` (STRING), `age` (STRING), `email` (STRING)
- Invalid XML rows return NULL for extracted fields
**Validation**: Verify graceful handling of invalid XML with valid sample

### Test 11: invalid_xml - Sample Schema Approach
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: invalid_xml
- **Parsing Method**: parseFromSchema
- **Sample Record**: (empty)
- **Sample Schema**: STRUCT<id: STRING, name: STRING, age: INT, email: STRING>

**Purpose**: Test invalid XML handling with schema
**Expected Result**: 
- All original columns preserved
- New column: `invalid_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `id` (STRING), `name` (STRING), `age` (INT64), `email` (STRING)
- Invalid XML rows return NULL for extracted fields
**Validation**: Verify schema-based invalid XML handling

## XML Structure Variation Tests

### Test 12: Deeply Nested XML Structure
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: complex_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<root>
  <user id="123" role="admin">
    <name>John Doe</name>
    <address>
      <street>123 Main St</street>
      <city>New York</city>
      <country>
        <name>United States</name>
        <code>US</code>
      </country>
    </address>
  </user>
  <orders>
    <order id="1" amount="99.99"/>
    <order id="2" amount="149.99"/>
  </orders>
</root>
```
- **Sample Schema**: (empty)

**Purpose**: Test deeply nested XML structure handling using actual data
**Expected Result**: 
- All original columns preserved
- New column: `complex_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `user` (STRUCT with nested `address` containing `country`), `orders` (XML_EXTRACT for array)
**Validation**: Verify deep nesting handling with proper structure preservation

### Test 13: Mixed Data Types XML Structure
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: xml_with_attributes
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<event id="301" type="conference" date="2024-03-15">
  <title>Tech Summit</title>
  <attendees max="500" current="350"/>
</event>
```
- **Sample Schema**: (empty)

**Purpose**: Test mixed data types in XML structure using actual data
**Expected Result**: 
- All original columns preserved
- New column: `xml_with_attributes_parsed` (STRUCT type with extracted fields)
- Extracted fields: `@attributes` (STRUCT with id, type, date), `title` (STRING), `attendees` (STRUCT with max, current)
**Validation**: Verify automatic type detection for STRING types and attribute handling

### Test 14: Simple XML Structure with Status and Score
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: simple_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<status>active</status>
<score>85</score>
```
- **Sample Schema**: (empty)

**Purpose**: Test simple XML structure with mixed data types using actual data
**Expected Result**: 
- All original columns preserved
- New column: `simple_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `status` (STRING), `score` (STRING)
**Validation**: Verify proper handling of STRING types

### Test 15: Array of Objects XML Structure
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: complex_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<products>
  <product id="1" name="Laptop" price="999.99" category="Electronics"/>
  <product id="2" name="Mouse" price="29.99" category="Electronics"/>
  <product id="3" name="Keyboard" price="79.99" category="Electronics"/>
</products>
```
- **Sample Schema**: (empty)

**Purpose**: Test array of objects XML structure using actual data
**Expected Result**: 
- All original columns preserved
- New column: `complex_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `product` (XML_EXTRACT for array containing objects with id, name, price, category)
**Validation**: Verify array of objects preservation with complete field structure

### Test 16: Complex Nested Structure with User and Orders
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: complex_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<root>
  <user id="456" role="customer" status="active">
    <name>Alice Johnson</name>
    <address>
      <street>321 Elm St</street>
      <city>Boston</city>
      <zip>02101</zip>
    </address>
  </user>
  <orders>
    <order id="4" amount="199.99" date="2024-01-15"/>
    <order id="5" amount="299.99" date="2024-01-16"/>
    <order id="6" amount="399.99" date="2024-01-17"/>
  </orders>
</root>
```
- **Sample Schema**: (empty)

**Purpose**: Test complex nested structure with user and orders using actual data
**Expected Result**: 
- All original columns preserved
- New column: `complex_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `user` (STRUCT with `@attributes` and nested `address`), `orders` (XML_EXTRACT for array with multiple order objects)
**Validation**: Verify handling of complex nested structure with arrays

### Test 17: Single Field XML Structure
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: simple_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<status>active</status>
```
- **Sample Schema**: (empty)

**Purpose**: Test single field XML structure using actual data
**Expected Result**: 
- All original columns preserved
- New column: `simple_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `status` (STRING)
**Validation**: Verify single field extraction

### Test 18: Complex XML with Multiple Orders
**Test Options:**
- **Input Table**: xml_parse_test_cases
- **Column Name**: complex_xml
- **Parsing Method**: parseFromSampleRecord
- **Sample Record**: 
```xml
<root>
  <user id="789" role="premium" status="active">
    <name>Bob Smith</name>
    <address>
      <street>456 Oak Ave</street>
      <city>Chicago</city>
      <zip>60601</zip>
    </address>
  </user>
  <orders>
    <order id="10" amount="199.99" date="2024-01-20" status="shipped"/>
    <order id="11" amount="299.99" date="2024-01-21" status="processing"/>
    <order id="12" amount="399.99" date="2024-01-22" status="delivered"/>
    <order id="13" amount="499.99" date="2024-01-23" status="pending"/>
  </orders>
</root>
```
- **Sample Schema**: (empty)

**Purpose**: Test complex XML structure with multiple orders using actual data
**Expected Result**: 
- All original columns preserved
- New column: `complex_xml_parsed` (STRUCT type with extracted fields)
- Extracted fields: `user` (STRUCT with `@attributes` and nested `address`), `orders` (XML_EXTRACT for array with multiple order objects)
**Validation**: Verify handling of complex nested structure with arrays

## Quick Copy/Paste Input Values for Gem UI

### Sample Records from Test Data (for parseFromSampleRecord method)

#### Simple XML Examples
```xml
<!-- Product Example -->
<product>
  <name>Widget Pro</name>
  <price>29.99</price>
  <category>Tools</category>
</product>

<!-- Person Example -->
<person>
  <name>John Doe</name>
  <age>30</age>
  <city>New York</city>
</person>

<!-- Book Example -->
<book>
  <title>Data Science Guide</title>
  <author>Grace Taylor</author>
  <pages>450</pages>
</book>
```

#### XML with Attributes
```xml
<!-- Event with Attributes -->
<event id="301" type="conference" date="2024-03-15">
  <title>Tech Summit</title>
  <attendees max="500" current="350"/>
</event>

<!-- Product with Attributes -->
<product id="123" category="electronics" price="299.99">
  <name>Laptop</name>
  <specs memory="16GB" storage="512GB"/>
</product>

<!-- User with Attributes -->
<user id="601" role="admin" permissions="full">
  <name>David Lee</name>
  <access level="admin" scope="global"/>
</user>
```

#### Complex Nested XML
```xml
<!-- Company Structure -->
<company>
  <name>Tech Corp</name>
  <employees>
    <employee>
      <id>101</id>
      <name>Bob Johnson</name>
      <department>Engineering</department>
      <projects>
        <project>
          <name>Website Redesign</name>
          <status>In Progress</status>
          <deadline>2024-06-30</deadline>
        </project>
      </projects>
    </employee>
  </employees>
</company>

<!-- Deep Nesting -->
<root>
  <level1>
    <level2>
      <level3>
        <level4>
          <level5>Deep nested content</level5>
        </level4>
      </level3>
    </level2>
  </level1>
</root>
```

#### XML with Namespaces
```xml
<!-- Data Namespace -->
<data:record xmlns:data="http://data.example.com/schema">
  <data:field>Data content</data:field>
</data:record>

<!-- Schema Namespace -->
<schema:table xmlns:schema="http://schema.example.com">
  <schema:column>Column definition</schema:column>
</schema:table>
```

#### XML with CDATA
```xml
<!-- JavaScript in CDATA -->
<script><![CDATA[function test() { if (x < 5 && y > 10) { alert('Hello'); } }]]></script>

<!-- SQL in CDATA -->
<query><![CDATA[SELECT name, age FROM users WHERE city = 'Miami' AND age > 25]]></query>

<!-- Description with CDATA -->
<description><![CDATA[This contains <tags> and & symbols that should not be parsed]]></description>
```

#### XML with Comments
```xml
<!-- Log with Comments -->
<log>
  <!-- Error occurred at 14:30 -->
  <error>Connection timeout</error>
  <!-- Retrying... -->
</log>

<!-- Document with Comments -->
<document>
  <!-- This is a comment -->
  <content>Main content</content>
  <!-- Another comment -->
</document>
```

#### XML with Special Characters
```xml
<!-- Math Symbols -->
<text>Math: 2² = 4, π ≈ 3.14</text>

<!-- Unicode Text -->
<text>Russian: Привет мир!</text>

<!-- Emojis -->
<text>Emojis: 🚀 💻 📊 🎯</text>
```

#### XML with Entities
```xml
<!-- HTML Entities -->
<html>&lt;h1&gt;Header&lt;/h1&gt; &amp; &lt;p&gt;Content&lt;/p&gt;</html>

<!-- XML Entities -->
<message>He said &quot;Hello World&quot; &amp; &lt;that&gt; was great!</message>

<!-- Nested XML as Entity -->
<xml>&lt;root&gt;&lt;element&gt;Content&lt;/element&gt;&lt;/root&gt;</xml>
```

#### Malformed XML (for error testing)
```xml
<!-- Missing closing tags -->
<product><name>Widget Pro<price>29.99</price><category>Tools</category>

<!-- Missing closing tags -->
<person><name>John Doe<age>30</age><city>New York</city>
```

### Sample Schemas (for parseFromSchema method)

#### Basic Schema
```sql
STRUCT<id: STRING, name: STRING, age: INT, email: STRING>
```

#### Complex Nested Schema
```sql
STRUCT<user: STRUCT<id: STRING, name: STRING, address: STRUCT<street: STRING, city: STRING>>, orders: ARRAY<STRUCT<id: STRING, amount: FLOAT>>>
```

#### Array Schema
```sql
ARRAY<STRUCT<id: STRING, name: STRING, price: FLOAT>>
```

#### Simple Schema
```sql
STRUCT<status: STRING, score: INT>
```

#### Deeply Nested Schema
```sql
STRUCT<user: STRUCT<id: STRING, name: STRING, address: STRUCT<street: STRING, city: STRING, country: STRUCT<name: STRING, code: STRING>>>, orders: ARRAY<STRUCT<id: STRING, amount: FLOAT>>>
```

## Test Inputs Summary Table

| Test | Input Table | Column Name | Parsing Method | Sample Record | Sample Schema |
|------|-------------|-------------|----------------|---------------|---------------|
| 1 | xml_parse_test_cases | (empty) | parseFromSchema | (empty) | STRUCT<name: STRING, age: INT> |
| 2 | xml_parse_test_cases | simple_xml | parseFromSampleRecord | `<product><name>Widget Pro</name><price>29.99</price><category>Tools</category></product>` | (empty) |
| 3 | xml_parse_test_cases | simple_xml | parseFromSchema | (empty) | STRUCT<name: STRING, price: FLOAT, category: STRING> |
| 4 | xml_parse_test_cases | complex_xml | parseFromSampleRecord | `<inventory><warehouse>North</warehouse><items><item><sku>SKU001</sku><name>Laptop</name><stock>50</stock><location><aisle>A</aisle><shelf>3</shelf></location></item><item><sku>SKU002</sku><name>Mouse</name><stock>200</stock><location><aisle>B</aisle><shelf>1</shelf></location></item></items></inventory>` | (empty) |
| 5 | xml_parse_test_cases | complex_xml | parseFromSchema | (empty) | STRUCT<user: STRUCT<id: STRING, name: STRING, address: STRUCT<street: STRING, city: STRING>>, orders: ARRAY<STRUCT<id: STRING, amount: FLOAT>>> |
| 6 | xml_parse_test_cases | complex_xml | parseFromSampleRecord | `<catalog><category>Electronics</category><products><product><id>701</id><name>Smartphone</name><brand>TechCorp</brand><price>699.99</price><specs><screen>6.1 inch</screen><camera>48MP</camera><storage>128GB</storage><ram>8GB</ram></specs><reviews><review><rating>5</rating><comment>Great phone!</comment><user>John</user></review><review><rating>4</rating><comment>Good value</comment><user>Jane</user></review></reviews></product></products></catalog>` | (empty) |
| 7 | xml_parse_test_cases | complex_xml | parseFromSchema | (empty) | STRUCT<category: STRING, products: STRUCT<product: STRUCT<id: STRING, name: STRING, brand: STRING, price: FLOAT, specs: STRUCT<screen: STRING, camera: STRING, storage: STRING, ram: STRING>, reviews: ARRAY<STRUCT<rating: INT, comment: STRING, user: STRING>>>>> |
| 8 | xml_parse_test_cases | simple_xml | parseFromSampleRecord | `<status>active</status><score>85</score>` | (empty) |
| 9 | xml_parse_test_cases | simple_xml | parseFromSchema | (empty) | STRUCT<status: STRING, score: INT> |
| 10 | xml_parse_test_cases | malformed_xml | parseFromSampleRecord | `<product><name>Widget Pro<price>29.99</price><category>Tools</category>` | (empty) |
| 11 | xml_parse_test_cases | malformed_xml | parseFromSchema | (empty) | STRUCT<name: STRING, price: FLOAT, category: STRING> |
| 12 | xml_parse_test_cases | complex_xml | parseFromSampleRecord | `<company><name>Tech Corp</name><employees><employee><id>101</id><name>Bob Johnson</name><department>Engineering</department><projects><project><name>Website Redesign</name><status>In Progress</status><deadline>2024-06-30</deadline></project></projects></employee></employees></company>` | (empty) |
| 13 | xml_parse_test_cases | simple_xml | parseFromSampleRecord | `<status>active</status>` | (empty) |
| 14 | xml_parse_test_cases | complex_xml | parseFromSampleRecord | `<catalog><category>Electronics</category><products><product><id>701</id><name>Smartphone</name><brand>TechCorp</brand><price>699.99</price><specs><screen>6.1 inch</screen><camera>48MP</camera><storage>128GB</storage><ram>8GB</ram></specs><reviews><review><rating>5</rating><comment>Great phone!</comment><user>John</user></review><review><rating>4</rating><comment>Good value</comment><user>Jane</user></review></reviews></product></products></catalog>` | (empty) |

## Key Features

### What This Gem Does:
- ✅ Uses BigQuery native string functions (REGEXP_EXTRACT, REGEXP_EXTRACT_ALL)
- ✅ No UDFs required - works with standard BigQuery functions
- ✅ Generates dynamic SQL based on sample record or schema structure
- ✅ **Structure Preservation**: Output structure matches the provided input structure (nested or flat)
- ✅ **Field Validation**: Sample records and schemas should match actual XML data structure
- ✅ **Attribute Handling**: XML attributes are stored in `@attributes` key
- ✅ **Text Content**: Mixed content is stored in `#text` key

### Important Notes:
- **✅ No UDF Required**: This gem uses only BigQuery native string functions
- **⚠️ Field Matching**: The sample record or schema fields should exist in the actual XML data being parsed
- **⚠️ Missing Fields**: If sample record fields don't exist in the XML data, they will return NULL values
- **✅ Data Validation**: Always verify that your sample record/schema matches the actual XML structure in your data
- ✅ Creates new column with `_parsed` suffix containing STRUCT with extracted fields
- ✅ Preserves all original columns
- ✅ Handles both parseFromSchema and parseFromSampleRecord methods
- ✅ Automatically detects field types (STRING, INT64, FLOAT64, BOOL, XML arrays)
- ✅ **Nested Structure Support**: Preserves nested structures when provided in sample record or schema
- ✅ **Flattened Structure Support**: Flattens structures when provided as flat sample record or schema
- ✅ Returns NULL for invalid XML strings
- ✅ Falls back to SELECT * if parsing fails
- ✅ No UDF dependencies required

### Data Type Conversion:
- **Input**: STRING column containing XML text
- **Output**: All original columns + STRUCT column with extracted fields
- **Field Types**: Automatically detected and cast (STRING, INT64, FLOAT64, BOOL)
- **Structure Preservation**: 
  - **Nested Input**: Preserves nested structure (e.g., `user.name`, `user.address.street`)
  - **Flat Input**: Produces flat structure (e.g., `user_name`, `user_address_street`)
- **Arrays**: Preserved as XML_EXTRACT for arrays
- **Attributes**: Stored in `@attributes` key as STRUCT
- **Text Content**: Stored in `#text` key for mixed content
- **Invalid XML**: Returns NULL for extracted fields
- **Empty XML**: Returns NULL for extracted fields

## Expected Results Summary

| Test | Input Column | Method | Output Column | Expected Result |
|------|-------------|--------|---------------|-----------------|
| 1 | (empty) | parseFromSchema | N/A | Fallback to SELECT * |
| 2 | simple_xml | parseFromSampleRecord | simple_xml_parsed | STRUCT with name, price, category fields |
| 3 | simple_xml | parseFromSchema | simple_xml_parsed | STRUCT with name, price, category fields |
| 4 | complex_xml | parseFromSampleRecord | complex_xml_parsed | STRUCT with nested user and orders fields |
| 5 | complex_xml | parseFromSchema | complex_xml_parsed | STRUCT with nested user and orders fields |
| 6 | complex_xml | parseFromSampleRecord | complex_xml_parsed | STRUCT with REGEXP_EXTRACT for complex nested structure |
| 7 | complex_xml | parseFromSchema | complex_xml_parsed | STRUCT with REGEXP_EXTRACT for complex nested structure |
| 8 | simple_xml | parseFromSampleRecord | simple_xml_parsed | STRUCT with status, score fields |
| 9 | simple_xml | parseFromSchema | simple_xml_parsed | STRUCT with status, score fields |
| 10 | malformed_xml | parseFromSampleRecord | malformed_xml_parsed | STRUCT with fields, NULL for invalid rows |
| 11 | malformed_xml | parseFromSchema | malformed_xml_parsed | STRUCT with fields, NULL for invalid rows |
| 12 | complex_xml | parseFromSampleRecord | complex_xml_parsed | STRUCT with deep nested fields |
| 13 | xml_with_attributes | parseFromSampleRecord | xml_with_attributes_parsed | STRUCT with mixed type fields |
| 14 | simple_xml | parseFromSampleRecord | simple_xml_parsed | STRUCT with edge case values |
| 15 | complex_xml | parseFromSampleRecord | complex_xml_parsed | STRUCT with complex nested objects |
| 16 | complex_xml | parseFromSampleRecord | complex_xml_parsed | STRUCT with complex mixed fields |
| 17 | simple_xml | parseFromSampleRecord | simple_xml_parsed | STRUCT with single field |
| 18 | complex_xml | parseFromSampleRecord | complex_xml_parsed | STRUCT with many fields |

## Validation Checklist

- [ ] Dynamic SQL generation works correctly
- [ ] Both parseFromSchema and parseFromSampleRecord methods work
- [ ] Fallback to SELECT * when parsing fails
- [ ] Empty column handling
- [ ] Complex XML structures are handled and extracted correctly
- [ ] XML arrays are preserved as XML_EXTRACT for arrays
- [ ] Invalid XML returns NULL for extracted fields
- [ ] Empty XML objects return NULL for extracted fields
- [ ] New column has `_parsed` suffix and contains STRUCT
- [ ] All original columns are preserved
- [ ] Field types are automatically detected and cast correctly
- [ ] Nested objects are handled with proper structure preservation
- [ ] No UDF dependencies required
- [ ] **NEW**: Each dataset column tested with both methods (simple_xml, complex_xml, nested_xml, malformed_xml, xml_with_attributes, xml_with_namespaces, xml_with_cdata, xml_with_comments, xml_with_special_chars, xml_with_entities)
- [ ] **NEW**: Deep nesting handling (4+ levels)
- [ ] **NEW**: Mixed data type detection (STRING, INT64, FLOAT64, BOOL, NULL, XML)
- [ ] **NEW**: Edge case value handling (empty objects, null values, zero values)
- [ ] **NEW**: Array of objects preservation
- [ ] **NEW**: Complex mixed structure handling
- [ ] **NEW**: Single value extraction
- [ ] **NEW**: Large structure handling (10+ fields)
- [ ] **NEW**: XML attribute handling (@attributes key)
- [ ] **NEW**: Mixed content handling (#text key)

## Advanced Testing Scenarios

### Method Consistency
Test that both parseFromSchema and parseFromSampleRecord produce identical results:
```sql
-- Both methods should produce the same STRUCT column
-- Input: '<person id="123"><name>John</name><age>30</age></person>'
-- Expected: STRUCT with @attributes (id), name (STRING) and age (STRING) fields
```

### Type Detection
Test automatic type detection from sample data:
```sql
-- String: "John" → STRING
-- Integer: 30 → INT64 (when cast)
-- Float: 30.5 → FLOAT64 (when cast)
-- Boolean: true → BOOL (when cast)
-- Array: <item>1</item><item>2</item> → XML_EXTRACT for array
```

### Nested Object Handling
Test handling of nested XML objects:
```sql
-- Input: '<user><name>John</name><address><city>NYC</city></address></user>'
-- Expected: user (STRUCT containing name and address), address (STRUCT containing city)
```

### Invalid XML Handling
Test behavior with various invalid XML strings:
```sql
-- Invalid XML: '<person><name>John</name><age></age></person>' 
-- Expected: NULL for extracted fields
-- Invalid XML: 'not xml at all'
-- Expected: NULL for extracted fields
```

### Edge Cases
Test edge cases for XML parsing:
```sql
-- Empty string: ''
-- Expected: NULL for extracted fields
-- Empty XML: '<root></root>'
-- Expected: NULL for extracted fields
-- XML array: '<items><item>1</item><item>2</item></items>'
-- Expected: XML_EXTRACT for array preserved
```

### Generated SQL Validation
Test that the generated SQL is syntactically correct:
```sql
-- Sample Record: '<person id="123"><name>John</name><age>30</age></person>'
-- Generated SQL should be:
-- STRUCT('123' AS `id`) AS `@attributes`,
-- XML_EXTRACT_SCALAR(xml_data, '//name') AS `name`,
-- XML_EXTRACT_SCALAR(xml_data, '//age') AS `age`
```

### XML Attribute Handling
Test proper handling of XML attributes:
```sql
-- Input: '<person id="123" active="true"><name>John</name></person>'
-- Expected: @attributes (STRUCT with id, active), name (STRING)
```

### Mixed Content Handling
Test handling of mixed content in XML:
```sql
-- Input: '<description>This is <b>bold</b> text</description>'
-- Expected: #text (STRING with "This is bold text"), b (STRING with "bold")
```

This testing guide ensures comprehensive validation of the XMLParse gem's native BigQuery XML parsing functionality.
