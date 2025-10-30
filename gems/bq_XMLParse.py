import json
import xml.etree.ElementTree as ET
from typing import Any
from collections import OrderedDict
from dataclasses import dataclass

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class XMLParse(MacroSpec):
    name: str = "XMLParse"
    projectName: str = "prophecy_basics"
    category: str = "Parse"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        # ProviderTypeEnum.Databricks
        # ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
        # ProviderTypeEnum.ProphecyManaged,
    ]

    @dataclass(frozen=True)
    class XMLParseProperties(MacroProperties):
        # properties for the component with default values
        columnName: str = ""
        relation_name: List[str] = field(default_factory=list)
        parsingMethod: str = "parseFromSampleRecord"
        sampleRecord: Optional[str] = None
        sampleSchema: Optional[str] = None
        generated_sql: str = ""
        processNamespaces: Optional[str] = None
        useOrderedDict: Optional[str] = None
        encoding: Optional[str] = None

    def get_relation_names(self, component: Component, context: SqlContext):
        all_upstream_nodes = []
        for inputPort in component.ports.inputs:
            upstreamNode = None
            for connection in context.graph.connections:
                if connection.targetPort == inputPort.id:
                    upstreamNodeId = connection.source
                    upstreamNode = context.graph.nodes.get(upstreamNodeId)
            all_upstream_nodes.append(upstreamNode)

        relation_name = []
        for upstream_node in all_upstream_nodes:
            if upstream_node is None or upstream_node.label is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.label)

        return relation_name

    def dialog(self) -> Dialog:
        methodRadioGroup = (
            RadioGroup("Parsing method")
            .addOption(
                "Parse from sample record",
                "parseFromSampleRecord",
                description=("Provide a sample record to parse the schema from"),
            )
            .addOption(
                "Parse from schema",
                "parseFromSchema",
                description="Provide sample schema in SQL struct format to parse the data with",
            )
            .setOptionType("button")
            .setVariant("medium")
            .setButtonStyle("solid")
            .bindProperty("parsingMethod")
        )

        sampleRecordTextXML = (
            TextArea("Sample XML record to parse schema from", 20)
            .bindProperty("sampleRecord")
            .bindPlaceholder(
                """<root>
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
</root>"""
            )
        )
        return Dialog("ColumnParser").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(TitleElement("Select Column to Parse"))
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%").addElement(
                                SchemaColumnsDropdown("", appearance="minimal")
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("columnName")
                                .showErrorsFor("columnName")
                            )
                        )
                    )
                )
                .addElement(methodRadioGroup)
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.parsingMethod"),
                        StringExpr("parseFromSampleRecord"),
                    )
                    .then(sampleRecordTextXML)
                )
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.parsingMethod"),
                        StringExpr("parseFromSchema"),
                    )
                    .then(
                        TextArea("Schema struct to parse the column", 20)
                        .bindProperty("sampleSchema")
                        .bindPlaceholder(
                            """STRUCT<
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
>"""
                        )
                    )
                ),
                "1fr",
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(XMLParse, self).validate(context, component)

        if (
            component.properties.columnName is None
            or component.properties.columnName == ""
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.columnName",
                    "Please select a column for the operation",
                    SeverityLevelEnum.Error,
                )
            )
        else:
            # Extract all column names from the schema
            field_names = [
                field["name"] for field in component.ports.inputs[0].schema["fields"]
            ]
            if component.properties.columnName not in field_names:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.columnName",
                        f"Selected column {component.properties.columnName} is not present in input schema.",
                        SeverityLevelEnum.Error,
                    )
                )

        if (
            component.properties.parsingMethod is None
            or component.properties.parsingMethod == ""
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.parsingMethod",
                    "Please select a parsing method",
                    SeverityLevelEnum.Error,
                )
            )
        else:
            if component.properties.parsingMethod == "parseFromSchema":
                if (
                    component.properties.sampleSchema is None
                    or component.properties.sampleSchema == ""
                ):
                    diagnostics.append(
                        Diagnostic(
                            "component.properties.sampleSchema",
                            "Please provide a valid SQL struct schema",
                            SeverityLevelEnum.Error,
                        )
                    )
            elif component.properties.parsingMethod == "parseFromSampleRecord":
                if (
                    component.properties.sampleRecord is None
                    or component.properties.sampleRecord == ""
                ):
                    diagnostics.append(
                        Diagnostic(
                            "component.properties.sampleRecord",
                            "Please provide a valid sample xml record",
                            SeverityLevelEnum.Error,
                        )
                    )
            else:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.parsingMethod",
                        "Invalid Parsing method selected",
                        SeverityLevelEnum.Error,
                    )
                )

        return diagnostics

    def onChange(
        self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
        """
        Handle changes in the component state and generate native SQL for XML parsing.
        
        This method performs two main functions:
        1. Updates the relation_name based on upstream connections
        2. Generates BigQuery native SQL for XML structure extraction based on sample record or schema
        
        The generated SQL uses BigQuery's built-in XML functions (XML_EXTRACT, XML_EXTRACT_SCALAR)
        instead of Python UDFs, providing better performance and simpler deployment.
        
        Args:
            context: SQL context for the component
            oldState: Previous state of the component
            newState: Current state of the component
            
        Returns:
            Component: Updated component with relation_name and generated SQL
        """
        # Handle changes in the newState's state and return the new state
        relation_name = self.get_relation_names(newState, context)
        
        # Generate native SQL based on the current properties
        generated_sql = self._generate_native_sql(newState.properties)
        
        return replace(
            newState,
            properties=replace(newState.properties, relation_name=relation_name, generated_sql=generated_sql),
        )

    def _generate_native_sql(self, properties) -> str:
        """
        Generate BigQuery native SQL for XML structure extraction based on current properties.
        
        This method analyzes the current component properties and generates appropriate
        BigQuery native SQL code for XML field extraction using XML_EXTRACT and XML_EXTRACT_SCALAR.
        
        Args:
            properties: The component properties containing parsing method and data
            
        Returns:
            str: Generated BigQuery SQL for XML extraction
        """
        if not properties.columnName or not properties.parsingMethod:
            return ""
            
        column_name = properties.columnName
        parsing_method = properties.parsingMethod
        sample_record = properties.sampleRecord if properties.sampleRecord else ""
        sample_schema = properties.sampleSchema if properties.sampleSchema else ""
        
        try:
            if parsing_method == "parseFromSampleRecord" and sample_record:
                # Parse XML sample and generate SQL
                sample_data = self._parse_xml_sample(sample_record)
                
                # Handle top-level arrays
                if isinstance(sample_data, list):
                    return f"XML_EXTRACT({column_name}, '//*')"
                
                return self._struct_from_xml(column_name, sample_data)
            elif parsing_method == "parseFromSchema" and sample_schema:
                # Handle schema parsing with top-level array support
                schema_upper = sample_schema.strip().upper()
                if schema_upper.startswith('ARRAY<'):
                    return f"XML_EXTRACT({column_name}, '//*')"
                if not schema_upper.startswith('STRUCT<'):
                    return "STRUCT()"
                return self._struct_from_schema(column_name, sample_schema)
        except Exception as e:
            return f"-- Error: {str(e)}"
        
        return ""

    def _parse_xml_sample(self, xml_string: str) -> dict:
        """
        Parse XML sample string into a dictionary structure.
        
        Args:
            xml_string: XML string to parse
            
        Returns:
            dict: Parsed XML as dictionary
        """
        try:
            root = ET.fromstring(xml_string)
            return self._element_to_dict(root)
        except Exception as e:
            raise Exception(f"Failed to parse XML sample: {str(e)}")

    def _element_to_dict(self, element) -> dict:
        """
        Convert XML element to dictionary with improved structure handling.
        
        Args:
            element: XML element to convert
            
        Returns:
            dict: Dictionary representation of XML element
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
        Generate BigQuery STRUCT SQL from parsed XML data.
        
        Args:
            col: Name of the column containing XML strings
            obj: Parsed XML data as a dictionary
            prefix: XML path prefix for nested objects
            
        Returns:
            str: Generated BigQuery STRUCT SQL
        """
        # Handle case where obj is not a dictionary (e.g., single field XML)
        if not isinstance(obj, dict):
            tag_name = prefix.split('/')[-1] if prefix else 'root'
            return self._extract_xml_value(col, tag_name)
        
        fields = []
        
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
        """Extract XML value using BigQuery string functions (no UDF required)."""
        # Use REGEXP_EXTRACT to get content between XML tags
        return f"REGEXP_EXTRACT({col}, r'<{tag_name}[^>]*>(.*?)</{tag_name}>')"
    
    def _extract_xml_attribute(self, col: str, tag_name: str, attr_name: str) -> str:
        """Extract XML attribute using BigQuery string functions."""
        # Use REGEXP_EXTRACT to get attribute value
        return f"REGEXP_EXTRACT({col}, r'<{tag_name}[^>]*{attr_name}=\"([^\"]*)\"')"

    def _get_sql_for_value(self, col: str, path: str, value: Any) -> str:
        """
        Consolidated type detection and SQL generation for any value.
        
        Args:
            col: Column name containing XML strings
            path: XML path for the value
            value: The value to analyze (dict, list, or primitive)
            
        Returns:
            str: Generated BigQuery SQL for the value
        """
        # Extract tag name from path (e.g., //name -> name)
        tag_name = path.replace('//', '') if path.startswith('//') else path
        
        if isinstance(value, dict):
            # Nested object - create nested STRUCT
            nested = self._struct_from_xml(col, value, path.replace('//', ''))
            return f"STRUCT(\n{nested}\n)"
        elif isinstance(value, list):
            # Array - use REGEXP_EXTRACT for multiple occurrences
            return f"REGEXP_EXTRACT_ALL({col}, r'<{tag_name}[^>]*>(.*?)</{tag_name}>')"
        elif isinstance(value, bool):
            # Boolean
            extracted = self._extract_xml_value(col, tag_name)
            return f"CAST({extracted} AS BOOL)"
        elif isinstance(value, int):
            # Integer
            extracted = self._extract_xml_value(col, tag_name)
            return f"CAST({extracted} AS INT64)"
        elif isinstance(value, float):
            # Float
            extracted = self._extract_xml_value(col, tag_name)
            return f"CAST({extracted} AS FLOAT64)"
        else:
            # String or other
            return self._extract_xml_value(col, tag_name)

    def _struct_from_schema(self, col: str, schema: str) -> str:
        """
        Generate BigQuery STRUCT SQL from schema definition.
        
        Args:
            col: Name of the column containing XML strings
            schema: BigQuery STRUCT schema string
            
        Returns:
            str: Generated BigQuery SQL
        """
        schema_upper = schema.strip().upper()
        
        if not schema_upper.startswith('STRUCT<'):
            return "STRUCT()"
        
        # Remove STRUCT< and >
        content = schema[7:-1].strip()
        if not content:
            return "STRUCT()"
        
        fields = []
        field_parts = content.split(',')
        
        for field in field_parts:
            field = field.strip()
            if ':' not in field:
                continue
                
            field_name, field_type = field.split(':', 1)
            field_name = field_name.strip()
            field_type = field_type.strip()
            
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
        Apply the XMLParse transformation and return the macro call.
        
        This method generates the final macro call with all the necessary parameters,
        including the generated SQL for native XML parsing. The macro will return
        a single column with the parsed XML result, or fall back to SELECT * if parsing fails.
        
        Args:
            props: The component properties
            
        Returns:
            str: The macro call string
        """
        # You can now access self.relation_name here
        resolved_macro_name = f"{self.projectName}.{self.name}"

        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        sampleRecord: str = props.sampleRecord if props.sampleRecord is not None else ""
        sampleSchema: str = props.sampleSchema if props.sampleSchema is not None else ""
        generated_sql: str = props.generated_sql if props.generated_sql is not None else ""

        # Escape the generated_sql by wrapping it in quotes and escaping internal quotes
        escaped_generated_sql = generated_sql.replace("'", "\\'") if generated_sql else ""

        arguments = [
            "'" + table_name + "'",
            "'" + props.columnName + "'",
            "'" + props.parsingMethod + "'",
            "'" + sampleRecord + "'",
            "'" + sampleSchema + "'",
            "'" + escaped_generated_sql + "'"
        ]
        params = ",".join([param for param in arguments])
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        print(f"The name of the parametersMap is {parametersMap}")
        return XMLParse.XMLParseProperties(
            relation_name=parametersMap.get("relation_name"),
            columnName=parametersMap.get("columnName"),
            parsingMethod=parametersMap.get("parsingMethod"),
            sampleRecord=parametersMap.get("sampleRecord"),
            sampleSchema=parametersMap.get("sampleSchema"),
            generated_sql=parametersMap.get("generated_sql"),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
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
        relation_name = self.get_relation_names(component, context)
        return replace(
            component,
            properties=replace(component.properties, relation_name=relation_name),
        )
