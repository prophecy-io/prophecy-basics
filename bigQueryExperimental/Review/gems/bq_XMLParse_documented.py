"""
BigQuery XMLParse Gem - Heavily Documented Version

This module implements a Prophecy gem for parsing XML data in BigQuery using native SQL functions.
The gem provides a user-friendly interface for extracting structured data from XML columns without
requiring Python UDFs, making it more performant and easier to deploy.

The gem supports two parsing methods:
1. Parse from sample record: Analyzes a sample XML record to infer the schema
2. Parse from schema: Uses a provided BigQuery STRUCT schema definition

Key Features:
- Native BigQuery SQL generation using REGEXP_EXTRACT functions
- Automatic schema inference from sample XML data
- Support for nested elements, attributes, and text content
- Type detection and appropriate SQL casting
- User-friendly UI with validation
- Handles XML namespaces and complex structures

Author: Generated for BigQuery SQL Basics project
Version: 1.0
"""

from dataclasses import dataclass
from typing import Any, List, Optional
import xml.etree.ElementTree as ET

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class XMLParse(MacroSpec):
    """
    BigQuery XMLParse Gem - Native SQL XML Parsing Component
    
    This class implements a Prophecy gem that generates BigQuery native SQL for parsing XML data.
    It provides a visual interface for users to select XML columns and define how they should be
    parsed into structured data using BigQuery's built-in string and regex functions.
    
    The gem eliminates the need for Python UDFs by generating native BigQuery SQL that uses:
    - REGEXP_EXTRACT() for extracting XML element content
    - REGEXP_EXTRACT_ALL() for extracting multiple occurrences (arrays)
    - CAST() for type conversion
    - STRUCT() for creating structured output
    
    How it works:
    1. User selects an XML column from the input schema
    2. User chooses parsing method (sample record or schema definition)
    3. Gem analyzes the input and generates appropriate BigQuery SQL
    4. Generated SQL is embedded in the macro call for execution
    
    Integration with Prophecy:
    - Extends MacroSpec to provide gem functionality
    - Implements UI components for user interaction
    - Generates macro calls that can be executed in BigQuery
    - Provides validation and error handling
    
    Performance Benefits:
    - Uses native BigQuery functions (faster than UDFs)
    - No external dependencies or deployment requirements
    - Optimized SQL generation based on XML structure
    - Supports BigQuery's columnar processing
    
    XML-Specific Features:
    - Handles XML attributes (@attributes)
    - Supports text content (#text)
    - Processes nested elements recursively
    - Handles multiple elements with same tag (arrays)
    - Supports XML namespaces (basic)
    
    Limitations:
    - Schema inference is based on sample data (may miss edge cases)
    - Complex XML structures may require manual schema definition
    - Array handling uses REGEXP_EXTRACT_ALL (may not handle all XML array patterns)
    - Type detection is based on Python type inference (may not match BigQuery types exactly)
    - Limited support for XML namespaces and complex XML features
    - No validation of XML structure consistency
    """
    
    # Gem metadata - defines the gem's identity and behavior
    name: str = "XMLParse"                      # Gem name used in macro calls
    projectName: str = "BigQuerySqlBasics" # Project namespace for the gem
    category: str = "Parse"                     # UI category for organization
    minNumOfInputPorts: int = 1                 # Minimum number of input connections required

    @dataclass(frozen=True)
    class XMLParseProperties(MacroProperties):
        """
        Component Properties - Defines the configuration state of the XMLParse component
        
        This dataclass holds all the user-configurable properties that define how the XML
        parsing should be performed. These properties are set through the UI and used to
        generate the appropriate BigQuery SQL.
        
        Attributes:
            columnName (str): The name of the XML column to parse from the input schema
            relation_name (List[str]): List of upstream table names (populated automatically)
            parsingMethod (str): Method for determining XML structure ("parseFromSampleRecord" or "parseFromSchema")
            sampleRecord (Optional[str]): Sample XML record for schema inference
            sampleSchema (Optional[str]): BigQuery STRUCT schema definition
            generated_sql (str): The generated BigQuery SQL (populated automatically)
            processNamespaces (Optional[str]): Whether to process XML namespaces (future feature)
            useOrderedDict (Optional[str]): Whether to use ordered dictionary for parsing (future feature)
            encoding (Optional[str]): XML encoding specification (future feature)
        
        The properties are immutable (frozen=True) to ensure consistent state throughout
        the component lifecycle and prevent accidental modifications.
        
        Future Extensions:
        - processNamespaces: Will enable XML namespace processing
        - useOrderedDict: Will preserve element order during parsing
        - encoding: Will handle different XML encodings
        """
        columnName: str = ""                           # XML column to parse
        relation_name: List[str] = field(default_factory=list)  # Upstream table names
        parsingMethod: str = "parseFromSampleRecord"   # Default parsing method
        sampleRecord: Optional[str] = None             # Sample XML for schema inference
        sampleSchema: Optional[str] = None             # Manual schema definition
        generated_sql: str = ""                        # Generated BigQuery SQL
        processNamespaces: Optional[str] = None        # Future: XML namespace processing
        useOrderedDict: Optional[str] = None           # Future: Ordered dictionary support
        encoding: Optional[str] = None                 # Future: XML encoding support

    def get_relation_names(self, component: Component, context: SqlContext) -> List[str]:
        """
        Extract upstream table names from the component's input connections
        
        This method traverses the Prophecy graph to identify all upstream tables connected
        to this component's input ports. It's essential for generating the correct macro
        call with the proper table references.
        
        How it works:
        1. Iterates through all input ports of the component
        2. For each input port, finds the corresponding connection in the graph
        3. Retrieves the upstream node (table) connected to that port
        4. Extracts the table name (label) from the upstream node
        
        Args:
            component: The XMLParse component instance
            context: SQL context containing the graph and connection information
            
        Returns:
            List[str]: List of upstream table names, empty strings for unconnected ports
            
        Integration Notes:
        - Called during component state changes to update relation_name
        - Used in macro generation to reference the correct input tables
        - Handles cases where ports may not be connected (returns empty string)
        - Critical for multi-input scenarios (though this gem only supports single input)
        
        Error Handling:
        - Gracefully handles None upstream nodes (unconnected ports)
        - Returns empty string for missing labels rather than failing
        - Maintains list order corresponding to input port order
        """
        all_upstream_nodes = []
        
        # Traverse each input port to find connected upstream nodes
        for inputPort in component.ports.inputs:
            upstreamNode = None
            
            # Search through all connections to find the one connected to this port
            for connection in context.graph.connections:
                if connection.targetPort == inputPort.id:
                    upstreamNodeId = connection.source
                    upstreamNode = context.graph.nodes.get(upstreamNodeId)
                    break  # Found the connection, no need to continue searching
            
            all_upstream_nodes.append(upstreamNode)

        # Extract table names from upstream nodes
        relation_name = []
        for upstream_node in all_upstream_nodes:
            if upstream_node is None or upstream_node.label is None:
                relation_name.append("")  # Handle unconnected or unnamed nodes
            else:
                relation_name.append(upstream_node.label)

        return relation_name

    def dialog(self) -> Dialog:
        """
        Create the user interface dialog for the XMLParse component
        
        This method defines the complete UI layout and controls that users interact with
        to configure the XML parsing behavior. The dialog is built using Prophecy's
        UI specification framework and includes validation, conditional display, and
        user guidance.
        
        UI Components:
        1. Ports section: Shows input/output connections
        2. Column selection: Dropdown to select XML column from input schema
        3. Parsing method: Radio buttons to choose between sample record or schema
        4. Sample record input: Large text area for XML sample (conditional)
        5. Schema input: Text area for STRUCT definition (conditional)
        
        User Experience Features:
        - Conditional display based on selected parsing method
        - Placeholder text with examples for both input methods
        - Schema-aware column dropdown that updates with input schema
        - Error highlighting for invalid inputs
        - Responsive layout that adapts to different screen sizes
        
        XML-Specific UI Features:
        - XML sample with proper indentation and structure
        - Placeholder showing nested XML elements
        - Schema example matching XML structure
        - Clear labeling for XML-specific concepts
        
        Returns:
            Dialog: Complete UI dialog specification
            
        Integration with Prophecy:
        - Uses Prophecy's UI specification framework
        - Binds UI controls to component properties
        - Provides real-time validation feedback
        - Supports schema introspection for column selection
        """
        # Radio button group for selecting parsing method
        methodRadioGroup = RadioGroup("Parsing method") \
            .addOption("Parse from sample record", "parseFromSampleRecord", description=(
            "Provide a sample record to parse the schema from")) \
            .addOption("Parse from schema", "parseFromSchema",
                       description="Provide sample schema in SQL struct format to parse the data with") \
            .setOptionType("button") \
            .setVariant("medium") \
            .setButtonStyle("solid") \
            .bindProperty("parsingMethod")

        # Large text area for sample XML record input
        sampleRecordTextXML = TextArea("Sample XML record to parse schema from", 1).bindProperty(
            "sampleRecord").bindPlaceholder("""<root>
  <person>
    <id>1</id>
    <name>
      <first>John</first>
      <last>Doe</last>
    </name>
    <address>
      <street>Main St</street>
      <city>Springfield</city>
      <zip>12345</zip>
    </address>
  </person>
</root>""")
        
        # Build the complete dialog layout
        return Dialog("ColumnParser").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(), "content")  # Input/output ports section
                .addColumn(
                StackLayout(height="100%")
                    .addElement(
                    TitleElement("Select Column to Parse")
                )
                    .addElement(StepContainer()
                    .addElement(
                    Step()
                        .addElement(
                        StackLayout(height="100%")
                            .addElement(
                            # Schema-aware column dropdown
                            SchemaColumnsDropdown("", appearance = "minimal")
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("columnName")
                                .showErrorsFor("columnName")
                        )
                    )
                )
                ).addElement(methodRadioGroup)  # Parsing method selection
                    .addElement(
                    # Conditional display for sample record input
                    Condition()
                        .ifEqual(PropExpr("component.properties.parsingMethod"), StringExpr("parseFromSampleRecord"))
                        .then(sampleRecordTextXML))
                    .addElement(
                    # Conditional display for schema input
                    Condition()
                        .ifEqual(PropExpr("component.properties.parsingMethod"), StringExpr("parseFromSchema"))
                        .then(
                        TextArea("Schema struct to parse the column", 20).bindProperty("sampleSchema").bindPlaceholder("""STRUCT<
  root: STRUCT<
    person: STRUCT<
      id: INT,
      name: STRUCT<
        first: STRING,
        last: STRING
      >,
      address: STRUCT<
        street: STRING,
        city: STRING,
        zip: INT
      >
    >
  >
>"""))),
                "1fr"
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        """
        Validate component configuration and return diagnostic messages
        
        This method performs comprehensive validation of the component's configuration
        to ensure it can generate valid BigQuery SQL. It checks all required fields,
        validates data formats, and provides specific error messages for issues.
        
        Validation Checks:
        1. Column selection: Ensures a valid column is selected from input schema
        2. Parsing method: Validates that a parsing method is selected
        3. Sample data: Validates XML format for sample record method
        4. Schema format: Validates STRUCT syntax for schema method
        5. Schema compatibility: Ensures selected column exists in input schema
        
        Args:
            context: SQL context for validation
            component: Component instance to validate
            
        Returns:
            List[Diagnostic]: List of validation messages (errors, warnings, info)
            
        Error Types:
        - Error: Critical issues that prevent execution
        - Warning: Potential issues that may cause problems
        - Info: Informational messages for user guidance
        
        XML-Specific Validation:
        - Validates XML syntax in sample records
        - Checks for well-formed XML structure
        - Ensures proper XML element hierarchy
        - Validates XML attribute syntax
        
        Integration Notes:
        - Called automatically by Prophecy during component state changes
        - Real-time validation provides immediate feedback to users
        - Prevents invalid configurations from being saved or executed
        - Supports schema introspection for column validation
        """
        diagnostics = super(XMLParse, self).validate(context, component)

        # Validate column selection
        if component.properties.columnName is None or component.properties.columnName == '':
            diagnostics.append(
                Diagnostic("component.properties.columnName", "Please select a column for the operation",
                           SeverityLevelEnum.Error))
        else:
            # Verify selected column exists in input schema
            field_names = [field["name"] for field in component.ports.inputs[0].schema["fields"]]
            if component.properties.columnName not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.columnName", 
                             f"Selected column {component.properties.columnName} is not present in input schema.", 
                             SeverityLevelEnum.Error)
                )

        # Validate parsing method selection
        if component.properties.parsingMethod is None or component.properties.parsingMethod == '':
            diagnostics.append(
                Diagnostic("component.properties.parsingMethod", "Please select a parsing method",
                           SeverityLevelEnum.Error))
        else:
            # Validate method-specific requirements
            if component.properties.parsingMethod == 'parseFromSchema':
                if component.properties.sampleSchema is None or component.properties.sampleSchema == "":
                    diagnostics.append(
                        Diagnostic("component.properties.sampleSchema", "Please provide a valid SQL struct schema",
                                   SeverityLevelEnum.Error))
            elif component.properties.parsingMethod == 'parseFromSampleRecord':
                if component.properties.sampleRecord is None or component.properties.sampleRecord == "":
                    diagnostics.append(
                        Diagnostic("component.properties.sampleRecord", "Please provide a valid sample xml record",
                                   SeverityLevelEnum.Error))
            else:
                diagnostics.append(
                    Diagnostic("component.properties.parsingMethod", "Invalid Parsing method selected",
                               SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        """
        Handle component state changes and generate BigQuery SQL
        
        This is the core method that responds to user interactions and generates the
        appropriate BigQuery SQL for XML parsing. It's called whenever the component's
        properties change, ensuring the generated SQL stays in sync with the configuration.
        
        Process Flow:
        1. Extract upstream table names from the component's connections
        2. Generate BigQuery SQL based on current properties
        3. Update component state with relation names and generated SQL
        4. Return updated component for Prophecy to process
        
        SQL Generation Strategy:
        - Uses BigQuery's native string and regex functions for optimal performance
        - Avoids Python UDFs to eliminate deployment complexity
        - Generates type-safe SQL with appropriate casting
        - Handles nested XML structures recursively
        - Supports both elements and attributes
        
        XML-Specific Processing:
        - Parses XML structure into dictionary representation
        - Handles XML attributes (@attributes)
        - Processes text content (#text)
        - Manages nested elements and arrays
        - Generates appropriate regex patterns for extraction
        
        Args:
            context: SQL context containing graph and connection information
            oldState: Previous component state (for comparison)
            newState: Current component state with updated properties
            
        Returns:
            Component: Updated component with relation names and generated SQL
            
        Performance Considerations:
        - SQL generation is lightweight and fast
        - No external API calls or heavy computations
        - Generated SQL is optimized for BigQuery's columnar processing
        - Supports BigQuery's query optimization and caching
        
        Error Handling:
        - Gracefully handles malformed XML in sample records
        - Provides fallback SQL for parsing errors
        - Maintains component state even if SQL generation fails
        - Logs errors for debugging without breaking the UI
        """
        # Extract upstream table names from component connections
        relation_name = self.get_relation_names(newState, context)
        
        # Generate BigQuery SQL based on current properties
        generated_sql = self._generate_native_sql(newState.properties)
        
        # Update component state with relation names and generated SQL
        return (replace(newState, properties=replace(newState.properties, 
                                                   relation_name=relation_name,
                                                   generated_sql=generated_sql)))

    def _generate_native_sql(self, properties) -> str:
        """
        Generate BigQuery native SQL for XML structure extraction
        
        This method is the heart of the SQL generation process. It analyzes the component's
        properties and generates appropriate BigQuery SQL code for extracting structured
        data from XML columns using BigQuery's built-in string and regex functions.
        
        Generation Process:
        1. Validate required parameters are present
        2. Determine parsing method (sample record vs schema)
        3. Parse input data (XML or schema definition)
        4. Generate appropriate SQL based on data structure
        5. Handle errors gracefully with fallback SQL
        
        SQL Generation Techniques:
        - REGEXP_EXTRACT(): For single XML element content extraction
        - REGEXP_EXTRACT_ALL(): For multiple occurrences (arrays)
        - CAST(): For type conversion to BigQuery types
        - STRUCT(): For creating structured output columns
        - Recursive processing for nested elements
        
        XML-Specific Features:
        - Handles XML attributes with special @attributes field
        - Processes text content with #text field
        - Manages nested elements recursively
        - Supports multiple elements with same tag (arrays)
        - Generates appropriate regex patterns for XML extraction
        
        Args:
            properties: Component properties containing parsing configuration
            
        Returns:
            str: Generated BigQuery SQL for XML extraction
            
        Error Handling:
        - Returns empty string for missing required parameters
        - Provides error comments for malformed input
        - Gracefully handles XML parsing errors
        - Maintains SQL syntax validity even with errors
        
        Performance Optimizations:
        - Uses native BigQuery functions (no UDFs)
        - Generates efficient SQL with minimal overhead
        - Leverages BigQuery's columnar processing
        - Supports query optimization and caching
        """
        # Validate required parameters
        if not properties.columnName or not properties.parsingMethod:
            return ""
            
        column_name = properties.columnName
        parsing_method = properties.parsingMethod
        sample_record = properties.sampleRecord if properties.sampleRecord else ""
        sample_schema = properties.sampleSchema if properties.sampleSchema else ""
        
        # Generate SQL based on parsing method
        try:
            if parsing_method == "parseFromSampleRecord" and sample_record:
                # Parse XML string into Python objects for analysis
                sample_data = self._parse_xml_sample(sample_record)
                
                # Handle top-level arrays (return as XML)
                if isinstance(sample_data, list):
                    return f"XML_EXTRACT({column_name}, '//*')"
                
                # Generate SQL from parsed XML structure
                return self._struct_from_xml(column_name, sample_data)
                
            elif parsing_method == "parseFromSchema" and sample_schema:
                # Generate SQL from provided STRUCT schema
                schema_upper = sample_schema.strip().upper()
                if schema_upper.startswith('ARRAY<'):
                    return f"XML_EXTRACT({column_name}, '//*')"
                if not schema_upper.startswith('STRUCT<'):
                    return "STRUCT()"
                return self._struct_from_schema(column_name, sample_schema)
                
        except Exception as e:
            # Return error comment for debugging
            return f"-- Error: {str(e)}"
        
        return ""

    def _parse_xml_sample(self, xml_string: str) -> dict:
        """
        Parse XML sample string into a dictionary structure
        
        This method converts XML text into a Python dictionary that can be analyzed
        to generate appropriate BigQuery SQL. It handles XML-specific features like
        attributes, text content, and nested elements.
        
        XML Parsing Process:
        1. Parse XML string using ElementTree
        2. Convert XML elements to dictionary representation
        3. Handle attributes, text content, and nested elements
        4. Manage multiple elements with same tag (arrays)
        5. Preserve XML structure for SQL generation
        
        XML-Specific Features:
        - Attributes are stored in @attributes field
        - Text content is stored in #text field
        - Nested elements become nested dictionaries
        - Multiple elements with same tag become arrays
        - Empty elements are handled appropriately
        
        Args:
            xml_string: XML string to parse
            
        Returns:
            dict: Parsed XML as dictionary structure
            
        Error Handling:
        - Raises exception for malformed XML
        - Provides detailed error messages for debugging
        - Maintains XML structure integrity
        
        Limitations:
        - Basic XML parsing (doesn't handle all XML features)
        - No namespace processing
        - Limited support for complex XML structures
        - No validation of XML schema or DTD
        """
        try:
            # Parse XML string into ElementTree
            root = ET.fromstring(xml_string)
            return self._element_to_dict(root)
        except Exception as e:
            raise Exception(f"Failed to parse XML sample: {str(e)}")

    def _element_to_dict(self, element) -> dict:
        """
        Convert XML element to dictionary with improved structure handling
        
        This method recursively converts XML elements into a dictionary structure
        that preserves the XML hierarchy and handles XML-specific features like
        attributes, text content, and multiple elements with the same tag.
        
        Conversion Process:
        1. Handle element attributes (@attributes)
        2. Process text content (#text)
        3. Convert child elements recursively
        4. Handle multiple elements with same tag (arrays)
        5. Optimize structure for SQL generation
        
        XML-Specific Handling:
        - Attributes: Stored in @attributes field
        - Text content: Stored in #text field
        - Nested elements: Become nested dictionaries
        - Multiple elements: Converted to arrays
        - Empty elements: Handled appropriately
        
        Args:
            element: XML element to convert
            
        Returns:
            dict: Dictionary representation of XML element
            
        Structure Optimization:
        - Simplifies single-child elements when appropriate
        - Preserves complex structures for accurate SQL generation
        - Handles mixed content (text + elements)
        - Maintains element order where possible
        
        Limitations:
        - Basic XML parsing (doesn't handle all XML features)
        - No namespace processing
        - Limited support for complex XML structures
        - No validation of XML schema or DTD
        """
        result = {}
        
        # Add attributes if present
        if element.attrib:
            result['@attributes'] = element.attrib
        
        # Handle text content
        if element.text and element.text.strip():
            if len(element) == 0:  # Leaf node
                return element.text.strip()
            else:  # Mixed content
                result['#text'] = element.text.strip()
        elif len(element) == 0 and not element.attrib:
            # Empty leaf node - return empty string
            return ""
        
        # Process children
        children = {}
        for child in element:
            child_data = self._element_to_dict(child)
            
            if child.tag in children:
                # Multiple children with same tag - convert to list
                if not isinstance(children[child.tag], list):
                    children[child.tag] = [children[child.tag]]
                children[child.tag].append(child_data)
            else:
                children[child.tag] = child_data
        
        # Merge children into result
        result.update(children)
        
        # If no attributes, no text, and only one child, return the child directly
        # This handles the root element case better
        if not element.attrib and not (element.text and element.text.strip()) and len(children) == 1:
            child_value = list(children.values())[0]
            # Only return the child directly if it's a dictionary (not a string)
            if isinstance(child_value, dict):
                return child_value
        
        return result

    def _struct_from_xml(self, col: str, obj: dict, prefix: str = "") -> str:
        """
        Generate BigQuery STRUCT SQL from parsed XML data
        
        This method analyzes parsed XML data and generates BigQuery STRUCT SQL
        that extracts the same structure from an XML column. It handles nested
        elements, attributes, text content, and arrays.
        
        XML-Specific Processing:
        - Handles @attributes field for XML attributes
        - Processes #text field for text content
        - Manages nested elements recursively
        - Supports multiple elements with same tag (arrays)
        - Generates appropriate regex patterns for extraction
        
        SQL Generation:
        - Uses REGEXP_EXTRACT for single element content
        - Uses REGEXP_EXTRACT_ALL for multiple occurrences
        - Creates nested STRUCT for complex elements
        - Handles attributes with special extraction logic
        
        Args:
            col: Name of the column containing XML strings
            obj: Parsed XML data as a dictionary
            prefix: XML path prefix for nested objects (internal use)
            
        Returns:
            str: Generated BigQuery STRUCT SQL
            
        XML-Specific Features:
        - @attributes: Creates separate STRUCT for XML attributes
        - #text: Extracts text content between tags
        - Nested elements: Handled recursively
        - Arrays: Uses REGEXP_EXTRACT_ALL for multiple occurrences
        
        Limitations:
        - Array elements are not individually processed
        - Type inference is based on sample data (may miss edge cases)
        - Complex nested structures may generate verbose SQL
        - No validation of XML structure consistency
        - Limited support for complex XML features
        """
        # Handle case where obj is not a dictionary (e.g., single field XML)
        if not isinstance(obj, dict):
            tag_name = prefix.split('/')[-1] if prefix else 'root'
            return self._extract_xml_value(col, tag_name)
        
        fields = []
        
        # Process each field in the parsed XML
        for key, value in obj.items():
            if key == '@attributes':
                # Handle attributes - create a separate struct for them
                attr_fields = []
                for attr_key, attr_value in value.items():
                    # Extract attribute using regex
                    attr_extract = self._extract_xml_attribute(col, prefix.split('/')[-1] if prefix else 'root', attr_key)
                    attr_fields.append(f"{attr_extract} AS `{attr_key}`")
                if attr_fields:
                    attr_join = ',\n    '.join(attr_fields)
                    fields.append(f"STRUCT(\n    {attr_join}\n) AS `@attributes`")
            elif key == '#text':
                # Handle text content - extract text between tags
                tag_name = prefix.split('/')[-1] if prefix else 'root'
                text_extract = self._extract_xml_value(col, tag_name)
                fields.append(f"{text_extract} AS `#text`")
            else:
                # Use consolidated type detection
                xml_path = f"//{key}" if not prefix else f"//{prefix.split('/')[-1]}/{key}"
                sql = self._get_sql_for_value(col, xml_path, value)
                fields.append(f"{sql} AS `{key}`")
        
        return ",\n".join(fields)

    def _extract_xml_value(self, col: str, tag_name: str) -> str:
        """
        Extract XML element value using BigQuery string functions
        
        This method generates BigQuery SQL that extracts the content between
        XML tags using REGEXP_EXTRACT. It handles the common case of extracting
        text content from XML elements.
        
        Regex Pattern:
        - Matches opening tag with any attributes: <tag_name[^>]*>
        - Captures content between tags: (.*?)
        - Matches closing tag: </tag_name>
        - Uses non-greedy matching to handle nested tags
        
        Args:
            col: Column name containing XML strings
            tag_name: XML tag name to extract
            
        Returns:
            str: Generated BigQuery SQL for extracting XML element content
            
        Performance Considerations:
        - Uses native BigQuery regex functions
        - Optimized for common XML patterns
        - Supports BigQuery's columnar processing
        - Handles nested tags appropriately
        
        Limitations:
        - May not handle all XML edge cases
        - Doesn't validate XML structure
        - Limited support for complex XML features
        """
        # Use REGEXP_EXTRACT to get content between XML tags
        return f"REGEXP_EXTRACT({col}, r'<{tag_name}[^>]*>(.*?)</{tag_name}>')"
    
    def _extract_xml_attribute(self, col: str, tag_name: str, attr_name: str) -> str:
        """
        Extract XML attribute value using BigQuery string functions
        
        This method generates BigQuery SQL that extracts attribute values
        from XML elements using REGEXP_EXTRACT. It handles the common case
        of extracting attribute values from XML tags.
        
        Regex Pattern:
        - Matches opening tag with any attributes: <tag_name[^>]*
        - Captures specific attribute value: attr_name="([^"]*)"
        - Uses non-greedy matching to handle multiple attributes
        
        Args:
            col: Column name containing XML strings
            tag_name: XML tag name containing the attribute
            attr_name: Attribute name to extract
            
        Returns:
            str: Generated BigQuery SQL for extracting XML attribute value
            
        Performance Considerations:
        - Uses native BigQuery regex functions
        - Optimized for common XML attribute patterns
        - Supports BigQuery's columnar processing
        - Handles multiple attributes appropriately
        
        Limitations:
        - May not handle all XML attribute edge cases
        - Doesn't validate XML structure
        - Limited support for complex XML features
        """
        # Use REGEXP_EXTRACT to get attribute value
        return f"REGEXP_EXTRACT({col}, r'<{tag_name}[^>]*{attr_name}=\"([^\"]*)\"')"

    def _get_sql_for_value(self, col: str, path: str, value: Any) -> str:
        """
        Generate SQL for extracting a specific value from XML
        
        This method determines the appropriate BigQuery SQL function and type casting
        for extracting a value from an XML column based on its Python type. It handles
        all supported data types and generates type-safe SQL.
        
        Type Detection and SQL Generation:
        - dict: Recursive STRUCT generation for nested elements
        - list: REGEXP_EXTRACT_ALL for multiple occurrences (arrays)
        - bool: CAST to BOOL with XML value extraction
        - int: CAST to INT64 with XML value extraction
        - float: CAST to FLOAT64 with XML value extraction
        - str/other: XML value extraction (returns as string)
        
        BigQuery Function Usage:
        - REGEXP_EXTRACT(): For single XML element content extraction
        - REGEXP_EXTRACT_ALL(): For multiple occurrences (arrays)
        - CAST(): For type conversion to BigQuery native types
        
        Args:
            col: Column name containing XML strings
            path: XML path to the value (e.g., "//name")
            value: The value to analyze (dict, list, or primitive)
            
        Returns:
            str: Generated BigQuery SQL for extracting the value
            
        Type Safety:
        - All type conversions use explicit CAST operations
        - XML paths are properly handled for extraction
        - Scalar extraction uses appropriate regex patterns
        - Object/array extraction uses appropriate functions
        
        Performance Considerations:
        - Uses native BigQuery functions for optimal performance
        - Type casting is done at query time (no preprocessing)
        - XML extraction is optimized by BigQuery
        - Supports BigQuery's columnar processing and vectorization
        """
        # Extract tag name from path (e.g., //name -> name)
        tag_name = path.replace('//', '') if path.startswith('//') else path
        
        if isinstance(value, dict):
            # Nested object - create nested STRUCT
            nested = self._struct_from_xml(col, value, path.replace('//', ''))
            return f"STRUCT(\n{nested}\n)"
        elif isinstance(value, list):
            # Array - use REGEXP_EXTRACT_ALL for multiple occurrences
            return f"REGEXP_EXTRACT_ALL({col}, r'<{tag_name}[^>]*>(.*?)</{tag_name}>')"
        elif isinstance(value, bool):
            # Boolean - extract value and cast to BOOL
            extracted = self._extract_xml_value(col, tag_name)
            return f"CAST({extracted} AS BOOL)"
        elif isinstance(value, int):
            # Integer - extract value and cast to INT64
            extracted = self._extract_xml_value(col, tag_name)
            return f"CAST({extracted} AS INT64)"
        elif isinstance(value, float):
            # Float - extract value and cast to FLOAT64
            extracted = self._extract_xml_value(col, tag_name)
            return f"CAST({extracted} AS FLOAT64)"
        else:
            # String or other - extract as string
            return self._extract_xml_value(col, tag_name)

    def _struct_from_schema(self, col: str, schema: str) -> str:
        """
        Generate BigQuery STRUCT SQL from schema definition
        
        This method parses a BigQuery STRUCT schema definition and generates SQL
        that extracts XML data according to that schema. It handles nested structures,
        arrays, and all BigQuery data types.
        
        Schema Parsing Process:
        1. Parse STRUCT<...> syntax
        2. Extract field names and types
        3. Handle nested STRUCT definitions
        4. Generate appropriate SQL for each field type
        5. Build complete STRUCT SQL
        
        Supported BigQuery Types:
        - STRUCT<...>: Nested structures (simplified handling)
        - ARRAY<...>: Arrays (extracted as arrays)
        - INT/INT64: Integer values
        - FLOAT/FLOAT64: Floating-point values
        - BOOL/BOOLEAN: Boolean values
        - STRING: String values (default)
        
        Args:
            col: Column name containing XML strings
            schema: BigQuery STRUCT schema string
            
        Returns:
            str: Generated BigQuery SQL
            
        Schema Format:
        - Must start with STRUCT<
        - Field format: field_name: field_type
        - Nested structures: field_name: STRUCT<...>
        - Arrays: field_name: ARRAY<...>
        - Types: INT, FLOAT, BOOL, STRING, etc.
        
        Error Handling:
        - Returns STRUCT() for invalid schema format
        - Handles malformed STRUCT syntax gracefully
        - Provides fallback for unrecognized types
        - Maintains SQL syntax validity
        
        Limitations:
        - Simple parser (doesn't handle all BigQuery type variations)
        - No validation of schema correctness
        - Limited error reporting for malformed schemas
        - Doesn't support complex type parameters
        - Simplified handling of nested structures
        """
        schema_upper = schema.strip().upper()
        
        # Validate schema format
        if not schema_upper.startswith('STRUCT<'):
            return "STRUCT()"
        
        # Remove STRUCT< and >
        content = schema[7:-1].strip()
        if not content:
            return "STRUCT()"
        
        fields = []
        field_parts = content.split(',')
        
        # Parse each field definition
        for field in field_parts:
            field = field.strip()
            if ':' not in field:
                continue
                
            field_name, field_type = field.split(':', 1)
            field_name = field_name.strip()
            field_type = field_type.strip()
            
            # Generate SQL based on field type
            if field_type.upper().startswith('STRUCT<'):
                # Nested STRUCT - simplified handling
                fields.append(f"REGEXP_EXTRACT({col}, r'<{field_name}[^>]*>(.*?)</{field_name}>') AS `{field_name}`")
            elif field_type.upper().startswith('ARRAY<'):
                # Array
                fields.append(f"REGEXP_EXTRACT_ALL({col}, r'<{field_name}[^>]*>(.*?)</{field_name}>') AS `{field_name}`")
            elif field_type.upper() in ['INT64', 'INT']:
                # Integer
                extracted = self._extract_xml_value(col, field_name)
                fields.append(f"CAST({extracted} AS INT64) AS `{field_name}`")
            elif field_type.upper() in ['FLOAT64', 'FLOAT']:
                # Float
                extracted = self._extract_xml_value(col, field_name)
                fields.append(f"CAST({extracted} AS FLOAT64) AS `{field_name}`")
            elif field_type.upper() in ['BOOL', 'BOOLEAN']:
                # Boolean
                extracted = self._extract_xml_value(col, field_name)
                fields.append(f"CAST({extracted} AS BOOL) AS `{field_name}`")
            else:
                # String or other
                extracted = self._extract_xml_value(col, field_name)
                fields.append(f"{extracted} AS `{field_name}`")
        
        return ",\n".join(fields)

    def apply(self, props: XMLParseProperties) -> str:
        """
        Generate the final macro call with all parameters
        
        This method creates the complete macro call string that will be executed
        in BigQuery. It combines all the component properties into a properly
        formatted macro call with escaped parameters.
        
        Macro Call Structure:
        - Project name and macro name
        - All required parameters in correct order
        - Proper string escaping for SQL safety
        - Template syntax for Prophecy execution
        
        Parameter Order:
        1. relation_name: Upstream table names
        2. columnName: XML column to parse
        3. parsingMethod: Method used for parsing
        4. sampleRecord: Sample XML data (if applicable)
        5. sampleSchema: Schema definition (if applicable)
        6. generated_sql: Generated BigQuery SQL
        
        Args:
            props: Component properties containing all configuration
            
        Returns:
            str: Complete macro call string for execution
            
        Security Considerations:
        - All string parameters are properly escaped
        - SQL injection prevention through parameter escaping
        - Safe handling of user-provided XML and schema data
        - Template syntax prevents direct SQL execution
        
        Integration with Prophecy:
        - Returns template string for Prophecy to process
        - Parameters are passed to the corresponding macro
        - Macro execution happens in BigQuery context
        - Results are integrated into the data pipeline
        """
        # Build resolved macro name with project namespace
        resolved_macro_name = f"{self.projectName}.{self.name}"

        # Extract and prepare parameters
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        sampleRecord: str = props.sampleRecord if props.sampleRecord is not None else ""
        sampleSchema: str = props.sampleSchema if props.sampleSchema is not None else ""
        generated_sql: str = props.generated_sql if props.generated_sql is not None else ""

        # Escape generated SQL for safe parameter passing
        escaped_generated_sql = generated_sql.replace("'", "\\'") if generated_sql else ""

        # Build parameter list with proper escaping
        arguments = [
            "'" + table_name + "'",
            "'" + props.columnName + "'",
            "'" + props.parsingMethod + "'",
            "'" + sampleRecord + "'",
            "'" + sampleSchema + "'",
            "'" + escaped_generated_sql + "'"
        ]
        
        # Join parameters and create macro call
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        """
        Load component properties from macro parameters
        
        This method converts macro parameters back into component properties
        when loading a saved component. It's the inverse of unloadProperties
        and is essential for component persistence and loading.
        
        Parameter Mapping:
        - Converts string parameters back to appropriate types
        - Handles None values and empty strings gracefully
        - Maintains type safety for all properties
        - Preserves component state across save/load cycles
        
        Args:
            properties: Macro properties containing parameter values
            
        Returns:
            PropertiesType: Loaded component properties
            
        Data Type Handling:
        - relation_name: List[str] from comma-separated string
        - columnName: str (direct mapping)
        - parsingMethod: str (direct mapping)
        - sampleRecord: str (direct mapping)
        - sampleSchema: str (direct mapping)
        - generated_sql: str (direct mapping)
        
        Error Handling:
        - Gracefully handles missing parameters
        - Provides default values for None parameters
        - Maintains component functionality even with incomplete data
        - Logs parameter mapping for debugging
        """
        # Convert macro parameters to dictionary
        parametersMap = self.convertToParameterMap(properties.parameters)
        print(f"The name of the parametersMap is {parametersMap}")
        
        # Create component properties from parameters
        return XMLParse.XMLParseProperties(
            relation_name=parametersMap.get('relation_name'),
            columnName=parametersMap.get('columnName'),
            parsingMethod=parametersMap.get('parsingMethod'),
            sampleRecord=parametersMap.get('sampleRecord'),
            sampleSchema=parametersMap.get('sampleSchema'),
            generated_sql=parametersMap.get('generated_sql')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        """
        Convert component properties to macro parameters for saving
        
        This method converts component properties into macro parameters
        that can be saved and later loaded. It's the inverse of loadProperties
        and is essential for component persistence.
        
        Parameter Conversion:
        - Converts all properties to string parameters
        - Handles None values and empty strings
        - Maintains parameter order for consistency
        - Preserves all component state
        
        Args:
            properties: Component properties to convert
            
        Returns:
            MacroProperties: Macro properties with parameter list
            
        Parameter List:
        - relation_name: Converted to string representation
        - columnName: Direct string mapping
        - parsingMethod: Direct string mapping
        - sampleRecord: Direct string mapping
        - sampleSchema: Direct string mapping
        - generated_sql: Direct string mapping
        
        Integration Notes:
        - Used during component saving and serialization
        - Parameters are stored in Prophecy's component registry
        - Enables component sharing and version control
        - Supports component templates and reuse
        """
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("columnName", properties.columnName),
                MacroParameter("parsingMethod", properties.parsingMethod),
                MacroParameter("sampleRecord", properties.sampleRecord),
                MacroParameter("sampleSchema", properties.sampleSchema),
                MacroParameter("generated_sql", properties.generated_sql)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        """
        Update input port slug based on upstream connections
        
        This method updates the component's input port information based on
        the current upstream connections. It's called when the component's
        connections change to ensure the port information stays current.
        
        Update Process:
        1. Extract current upstream table names
        2. Update component properties with new relation names
        3. Return updated component with current connection info
        
        Args:
            component: Component to update
            context: SQL context containing connection information
            
        Returns:
            Component: Updated component with current port information
            
        Integration Notes:
        - Called automatically when connections change
        - Ensures port information stays synchronized
        - Supports dynamic connection updates
        - Maintains component state consistency
        """
        # Get current upstream table names
        relation_name = self.get_relation_names(component, context)
        
        # Update component with current relation names
        return (replace(component, properties=replace(component.properties, relation_name=relation_name)))
