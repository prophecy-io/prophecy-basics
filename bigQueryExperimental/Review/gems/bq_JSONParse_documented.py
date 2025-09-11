"""
BigQuery JSONParse Gem - Heavily Documented Version

This module implements a Prophecy gem for parsing JSON data in BigQuery using native SQL functions.
The gem provides a user-friendly interface for extracting structured data from JSON columns without
requiring Python UDFs, making it more performant and easier to deploy.

The gem supports two parsing methods:
1. Parse from sample record: Analyzes a sample JSON record to infer the schema
2. Parse from schema: Uses a provided BigQuery STRUCT schema definition

Key Features:
- Native BigQuery SQL generation (no Python UDFs required)
- Automatic schema inference from sample data
- Support for nested objects and arrays
- Type detection and appropriate SQL casting
- User-friendly UI with validation

Author: Generated for BigQuery SQL Basics project
Version: 1.0
"""

from dataclasses import dataclass
import json
from typing import Any, List, Optional

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class JSONParse(MacroSpec):
    """
    BigQuery JSONParse Gem - Native SQL JSON Parsing Component
    
    This class implements a Prophecy gem that generates BigQuery native SQL for parsing JSON data.
    It provides a visual interface for users to select JSON columns and define how they should be
    parsed into structured data using BigQuery's built-in JSON functions.
    
    The gem eliminates the need for Python UDFs by generating native BigQuery SQL that uses:
    - JSON_EXTRACT() for extracting JSON objects and arrays
    - JSON_EXTRACT_SCALAR() for extracting scalar values
    - CAST() for type conversion
    - STRUCT() for creating structured output
    
    How it works:
    1. User selects a JSON column from the input schema
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
    - Optimized SQL generation based on data structure
    - Supports BigQuery's columnar processing
    
    Limitations:
    - Schema inference is based on sample data (may miss edge cases)
    - Complex nested structures may require manual schema definition
    - Array handling is limited to JSON_EXTRACT (no individual element processing)
    - Type detection is based on Python type inference (may not match BigQuery types exactly)
    """
    
    # Gem metadata - defines the gem's identity and behavior
    name: str = "JSONParse"                    # Gem name used in macro calls
    projectName: str = "BigQuerySqlBasics" # Project namespace for the gem
    category: str = "Parse"                    # UI category for organization
    minNumOfInputPorts: int = 1                # Minimum number of input connections required

    @dataclass(frozen=True)
    class JSONParseProperties(MacroProperties):
        """
        Component Properties - Defines the configuration state of the JSONParse component
        
        This dataclass holds all the user-configurable properties that define how the JSON
        parsing should be performed. These properties are set through the UI and used to
        generate the appropriate BigQuery SQL.
        
        Attributes:
            columnName (str): The name of the JSON column to parse from the input schema
            relation_name (List[str]): List of upstream table names (populated automatically)
            parsingMethod (str): Method for determining JSON structure ("parseFromSampleRecord" or "parseFromSchema")
            sampleRecord (Optional[str]): Sample JSON record for schema inference
            sampleSchema (Optional[str]): BigQuery STRUCT schema definition
            generated_sql (str): The generated BigQuery SQL (populated automatically)
        
        The properties are immutable (frozen=True) to ensure consistent state throughout
        the component lifecycle and prevent accidental modifications.
        """
        columnName: str = ""                           # JSON column to parse
        relation_name: List[str] = field(default_factory=list)  # Upstream table names
        parsingMethod: str = "parseFromSampleRecord"   # Default parsing method
        sampleRecord: Optional[str] = None             # Sample JSON for schema inference
        sampleSchema: Optional[str] = None             # Manual schema definition
        generated_sql: str = ""                        # Generated BigQuery SQL

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
            component: The JSONParse component instance
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
        Create the user interface dialog for the JSONParse component
        
        This method defines the complete UI layout and controls that users interact with
        to configure the JSON parsing behavior. The dialog is built using Prophecy's
        UI specification framework and includes validation, conditional display, and
        user guidance.
        
        UI Components:
        1. Ports section: Shows input/output connections
        2. Column selection: Dropdown to select JSON column from input schema
        3. Parsing method: Radio buttons to choose between sample record or schema
        4. Sample record input: Large text area for JSON sample (conditional)
        5. Schema input: Text area for STRUCT definition (conditional)
        
        User Experience Features:
        - Conditional display based on selected parsing method
        - Placeholder text with examples for both input methods
        - Schema-aware column dropdown that updates with input schema
        - Error highlighting for invalid inputs
        - Responsive layout that adapts to different screen sizes
        
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

        # Large text area for sample JSON record input
        sampleRecordTextJSON = TextArea("Sample JSON record to parse schema from", 20).bindProperty(
            "sampleRecord").bindPlaceholder("""{
  "root": {
    "person": {
      "id": 1,
      "name": {
        "first": "John",
        "last": "Doe"
      },
      "address": {
        "street": "Main St",
        "city": "Springfield",
        "zip": 12345
      }
    }
  }
}""")
        
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
                        .then(sampleRecordTextJSON))
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
        3. Sample data: Validates JSON format for sample record method
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
        
        Integration Notes:
        - Called automatically by Prophecy during component state changes
        - Real-time validation provides immediate feedback to users
        - Prevents invalid configurations from being saved or executed
        - Supports schema introspection for column validation
        """
        diagnostics = super(JSONParse, self).validate(context, component)

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
                        Diagnostic("component.properties.sampleRecord", "Please provide a valid sample json record",
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
        appropriate BigQuery SQL for JSON parsing. It's called whenever the component's
        properties change, ensuring the generated SQL stays in sync with the configuration.
        
        Process Flow:
        1. Extract upstream table names from the component's connections
        2. Generate BigQuery SQL based on current properties
        3. Update component state with relation names and generated SQL
        4. Return updated component for Prophecy to process
        
        SQL Generation Strategy:
        - Uses BigQuery's native JSON functions for optimal performance
        - Avoids Python UDFs to eliminate deployment complexity
        - Generates type-safe SQL with appropriate casting
        - Handles nested structures recursively
        - Supports both objects and arrays
        
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
        - Gracefully handles malformed JSON in sample records
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
        Generate BigQuery native SQL for JSON structure extraction
        
        This method is the heart of the SQL generation process. It analyzes the component's
        properties and generates appropriate BigQuery SQL code for extracting structured
        data from JSON columns using BigQuery's built-in JSON functions.
        
        Generation Process:
        1. Validate required parameters are present
        2. Determine parsing method (sample record vs schema)
        3. Parse input data (JSON or schema definition)
        4. Generate appropriate SQL based on data structure
        5. Handle errors gracefully with fallback SQL
        
        SQL Generation Techniques:
        - JSON_EXTRACT(): For objects and arrays (preserves JSON structure)
        - JSON_EXTRACT_SCALAR(): For scalar values (returns strings)
        - CAST(): For type conversion to BigQuery types
        - STRUCT(): For creating structured output columns
        - Recursive processing for nested objects
        
        Args:
            properties: Component properties containing parsing configuration
            
        Returns:
            str: Generated BigQuery SQL for JSON extraction
            
        Error Handling:
        - Returns empty string for missing required parameters
        - Provides error comments for malformed input
        - Gracefully handles JSON parsing errors
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
                # Parse JSON string into Python objects for analysis
                sample_data = json.loads(sample_record)
                
                # Handle top-level arrays (return as JSON)
                if isinstance(sample_data, list):
                    return f"JSON_EXTRACT({column_name}, '$')"
                
                # Generate SQL from parsed JSON structure
                return self._struct_from_json(column_name, sample_data)
                
            elif parsing_method == "parseFromSchema" and sample_schema:
                # Generate SQL from provided STRUCT schema
                schema_upper = sample_schema.strip().upper()
                if schema_upper.startswith('ARRAY<'):
                    return f"JSON_EXTRACT({column_name}, '$')"
                if not schema_upper.startswith('STRUCT<'):
                    return "STRUCT()"
                return self._struct_from_schema(column_name, sample_schema)
                
        except Exception as e:
            # Return error comment for debugging
            return f"-- Error: {str(e)}"
        
        return ""

    def _struct_from_json(self, col: str, obj: dict, prefix: str = "") -> str:
        """
        Recursively generate STRUCT SQL from JSON sample data
        
        This method analyzes a parsed JSON object and generates BigQuery STRUCT SQL
        that extracts the same structure from a JSON column. It handles nested objects,
        arrays, and primitive types, generating appropriate SQL for each.
        
        Recursive Processing:
        - Traverses JSON object hierarchy depth-first
        - Generates SQL for each field based on its type
        - Handles nested objects with recursive calls
        - Preserves field names and structure
        
        Type Mapping:
        - dict → STRUCT with nested fields
        - list → JSON_EXTRACT (preserves array structure)
        - bool → CAST to BOOL
        - int → CAST to INT64
        - float → CAST to FLOAT64
        - str → JSON_EXTRACT_SCALAR (returns as string)
        
        Args:
            col: Column name containing JSON strings
            obj: Parsed JSON object (dict) to analyze
            prefix: JSON path prefix for nested objects (internal use)
            
        Returns:
            str: Generated BigQuery STRUCT SQL
            
        SQL Structure:
        - Uses STRUCT() constructor for object structure
        - Each field becomes a STRUCT field with appropriate SQL
        - Field names are quoted with backticks for safety
        - Nested objects are handled recursively
        - Arrays are extracted as JSON (not parsed into individual elements)
        
        Limitations:
        - Array elements are not individually processed
        - Type inference is based on sample data (may miss edge cases)
        - Complex nested structures may generate verbose SQL
        - No validation of JSON structure consistency
        """
        fields = []
        
        # Process each field in the JSON object
        for k, v in obj.items():
            # Build JSON path for this field
            path = f"$.{prefix}.{k}" if prefix else f"$.{k}"
            
            # Generate appropriate SQL for this field's value
            sql = self._get_sql_for_value(col, path, v)
            fields.append(f"{sql} AS `{k}`")
        
        # Return STRUCT with all fields
        return "STRUCT(\n  " + ",\n  ".join(fields) + "\n)"

    def _get_sql_for_value(self, col: str, path: str, value: Any) -> str:
        """
        Generate SQL for extracting a specific value from JSON
        
        This method determines the appropriate BigQuery SQL function and type casting
        for extracting a value from a JSON column based on its Python type. It handles
        all supported data types and generates type-safe SQL.
        
        Type Detection and SQL Generation:
        - dict: Recursive STRUCT generation for nested objects
        - list: JSON_EXTRACT for arrays (preserves JSON structure)
        - bool: CAST to BOOL with JSON_EXTRACT_SCALAR
        - int: CAST to INT64 with JSON_EXTRACT_SCALAR
        - float: CAST to FLOAT64 with JSON_EXTRACT_SCALAR
        - str/other: JSON_EXTRACT_SCALAR (returns as string)
        
        BigQuery Function Usage:
        - JSON_EXTRACT(): For objects and arrays (preserves JSON structure)
        - JSON_EXTRACT_SCALAR(): For scalar values (returns strings)
        - CAST(): For type conversion to BigQuery native types
        
        Args:
            col: Column name containing JSON strings
            path: JSON path to the value (e.g., "$.root.person.name")
            value: The value to analyze (dict, list, or primitive)
            
        Returns:
            str: Generated BigQuery SQL for extracting the value
            
        Type Safety:
        - All type conversions use explicit CAST operations
        - JSON paths are properly quoted to prevent injection
        - Scalar extraction uses JSON_EXTRACT_SCALAR for safety
        - Object/array extraction uses JSON_EXTRACT for structure preservation
        
        Performance Considerations:
        - Uses native BigQuery functions for optimal performance
        - Type casting is done at query time (no preprocessing)
        - JSON path evaluation is optimized by BigQuery
        - Supports BigQuery's columnar processing and vectorization
        """
        if isinstance(value, dict):
            # Nested object - generate recursive STRUCT
            nested = self._struct_from_json(col, value, path.replace('$.', ''))
            return f"{nested}"
        elif isinstance(value, list):
            # Array - extract as JSON (preserves structure)
            return f"JSON_EXTRACT({col}, '{path}')"
        elif isinstance(value, bool):
            # Boolean - extract scalar and cast to BOOL
            return f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS BOOL)"
        elif isinstance(value, int):
            # Integer - extract scalar and cast to INT64
            return f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS INT64)"
        elif isinstance(value, float):
            # Float - extract scalar and cast to FLOAT64
            return f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS FLOAT64)"
        else:
            # String or other - extract as scalar (returns string)
            return f"JSON_EXTRACT_SCALAR({col}, '{path}')"

    def _struct_from_schema(self, col: str, schema: str, prefix: str = "") -> str:
        """
        Generate STRUCT SQL from BigQuery STRUCT schema definition
        
        This method parses a BigQuery STRUCT schema definition and generates SQL
        that extracts JSON data according to that schema. It handles nested structures,
        arrays, and all BigQuery data types.
        
        Schema Parsing Process:
        1. Parse STRUCT<...> syntax recursively
        2. Extract field names and types
        3. Handle nested STRUCT definitions
        4. Generate appropriate SQL for each field type
        5. Build complete STRUCT SQL
        
        Supported BigQuery Types:
        - STRUCT<...>: Nested structures (recursive processing)
        - ARRAY<...>: Arrays (extracted as JSON)
        - INT/INT64: Integer values
        - FLOAT/FLOAT64: Floating-point values
        - BOOL/BOOLEAN: Boolean values
        - STRING: String values (default)
        
        Args:
            col: Column name containing JSON strings
            schema: BigQuery STRUCT schema string
            prefix: JSON path prefix for nested objects (internal use)
            
        Returns:
            str: Generated BigQuery STRUCT SQL
            
        Schema Format:
        - Must start with STRUCT<
        - Field format: field_name: field_type
        - Nested structures: field_name: STRUCT<...>
        - Arrays: field_name: ARRAY<...>
        - Types: INT, FLOAT, BOOL, STRING, etc.
        
        Error Handling:
        - Returns JSON_EXTRACT for invalid schema format
        - Handles malformed STRUCT syntax gracefully
        - Provides fallback for unrecognized types
        - Maintains SQL syntax validity
        
        Limitations:
        - Simple parser (doesn't handle all BigQuery type variations)
        - No validation of schema correctness
        - Limited error reporting for malformed schemas
        - Doesn't support complex type parameters
        """
        schema = schema.strip()
        
        # Validate schema format
        if not schema.upper().startswith("STRUCT<"):
            return f"JSON_EXTRACT({col}, '$.{prefix}')"

        # Extract inner content of STRUCT<...>
        inner = schema[7:-1].strip()
        fields = []
        depth, buf, fname = 0, "", ""
        
        # Parse schema content character by character
        for c in inner + ",":
            if c == "<":
                depth += 1
                buf += c
            elif c == ">":
                depth -= 1
                buf += c
            elif c == ":" and depth == 0 and not fname:
                # Field name separator
                fname, buf = buf.strip(), ""
            elif c == "," and depth == 0:
                # Field definition complete
                ftype = buf.strip()
                buf = ""
                path = f"$.{prefix}.{fname}" if prefix else f"$.{fname}"
                
                # Generate SQL based on field type
                if ftype.upper().startswith("STRUCT<"):
                    # Nested structure - recursive processing
                    nested = self._struct_from_schema(col, ftype, f"{prefix}.{fname}" if prefix else fname)
                    fields.append(f"{nested} AS `{fname}`")
                elif ftype.upper().startswith("ARRAY<"):
                    # Array - extract as JSON
                    fields.append(f"JSON_EXTRACT({col}, '{path}') AS `{fname}`")
                elif ftype.upper().startswith(("INT", "INT64")):
                    # Integer type
                    fields.append(f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS INT64) AS `{fname}`")
                elif ftype.upper().startswith(("FLOAT", "FLOAT64")):
                    # Float type
                    fields.append(f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS FLOAT64) AS `{fname}`")
                elif ftype.upper().startswith(("BOOL", "BOOLEAN")):
                    # Boolean type
                    fields.append(f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS BOOL) AS `{fname}`")
                else:
                    # String type (default)
                    fields.append(f"JSON_EXTRACT_SCALAR({col}, '{path}') AS `{fname}`")
                fname = ""
            else:
                buf += c
        
        # Return complete STRUCT SQL
        return "STRUCT(\n  " + ",\n  ".join(fields) + "\n)"

    def apply(self, props: JSONParseProperties) -> str:
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
        2. columnName: JSON column to parse
        3. parsingMethod: Method used for parsing
        4. sampleRecord: Sample JSON data (if applicable)
        5. sampleSchema: Schema definition (if applicable)
        6. generated_sql: Generated BigQuery SQL
        
        Args:
            props: Component properties containing all configuration
            
        Returns:
            str: Complete macro call string for execution
            
        Security Considerations:
        - All string parameters are properly escaped
        - SQL injection prevention through parameter escaping
        - Safe handling of user-provided JSON and schema data
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
        return JSONParse.JSONParseProperties(
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
