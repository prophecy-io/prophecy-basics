import json
from collections import OrderedDict
from dataclasses import dataclass

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class JSONParse(MacroSpec):
    name: str = "JSONParse"
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
    class JSONParseProperties(MacroProperties):
        # properties for the component with default values
        columnName: str = ""
        relation_name: List[str] = field(default_factory=list)
        parsingMethod: str = "parseFromSampleRecord"
        sampleRecord: Optional[str] = None
        sampleSchema: Optional[str] = None
        generated_sql: str = ""

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

        sampleRecordTextJSON = (
            TextArea("Sample JSON record to parse schema from", 20)
            .bindProperty("sampleRecord")
            .bindPlaceholder(
                """{
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
}"""
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
                    .then(sampleRecordTextJSON)
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
        diagnostics = super(JSONParse, self).validate(context, component)

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
                            "Please provide a valid sample json record",
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
        Handle changes in the component state and generate native SQL for JSON parsing.
        
        This method performs two main functions:
        1. Updates the relation_name based on upstream connections
        2. Generates BigQuery native SQL for JSON structure extraction based on sample record or schema
        
        The generated SQL uses BigQuery's built-in JSON functions (JSON_EXTRACT, JSON_EXTRACT_SCALAR)
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
            properties=replace(newState.properties, relation_name=relation_name, generated_sql=generated_sql,),
        )

    def _generate_native_sql(self, properties) -> str:
        """
        Generate BigQuery native SQL for JSON structure extraction based on current properties.
        
        This method analyzes the current component properties and generates appropriate
        BigQuery native SQL code for JSON field extraction.
        
        Args:
            properties: The component properties containing parsing method and data
            
        Returns:
            str: Generated BigQuery SQL for JSON extraction
        """
        # Check if we have the required parameters
        if not properties.columnName or not properties.parsingMethod:
            return ""
            
        column_name = properties.columnName
        parsing_method = properties.parsingMethod
        sample_record = properties.sampleRecord if properties.sampleRecord else ""
        sample_schema = properties.sampleSchema if properties.sampleSchema else ""
        
        # Generate SQL based on the parsing method
        try:
            if parsing_method == "parseFromSampleRecord" and sample_record:
                # Parse JSON string into Python objects
                sample_data = json.loads(sample_record)
                
                # Handle top-level arrays
                if isinstance(sample_data, list):
                    return f"JSON_EXTRACT({column_name}, '$')"
                
                # Generate SQL directly from parsed data
                return self._struct_from_json(column_name, sample_data)
            elif parsing_method == "parseFromSchema" and sample_schema:
                # Handle schema parsing inline
                schema_upper = sample_schema.strip().upper()
                if schema_upper.startswith('ARRAY<'):
                    return f"JSON_EXTRACT({column_name}, '$')"
                if not schema_upper.startswith('STRUCT<'):
                    return "STRUCT()"
                return self._struct_from_schema(column_name, sample_schema)
        except Exception as e:
            return f"-- Error: {str(e)}"
        
        return ""
    

    def _struct_from_json(self, col: str, obj: dict, prefix: str = "") -> str:
        """
        Recursively build STRUCT SQL from a JSON sample.
        
        Args:
            col: Column name containing JSON strings
            obj: Parsed JSON object (dict)
            prefix: JSON path prefix for nested objects
            
        Returns:
            str: Generated BigQuery STRUCT SQL
        """
        fields = []
        for k, v in obj.items():
            path = f"$.{prefix}.{k}" if prefix else f"$.{k}"
            sql = self._get_sql_for_value(col, path, v)
            fields.append(f"{sql} AS `{k}`")
        
        return "STRUCT(\n  " + ",\n  ".join(fields) + "\n)"

    def _get_sql_for_value(self, col: str, path: str, value: Any) -> str:
        """
        Consolidated type detection and SQL generation for any value.
        
        Args:
            col: Column name containing JSON strings
            path: JSON path for the value
            value: The value to analyze (dict, list, or primitive)
            
        Returns:
            str: Generated BigQuery SQL for the value
        """
        if isinstance(value, dict):
            # Nested object - recursive call
            nested = self._struct_from_json(col, value, path.replace('$.', ''))
            return f"{nested}"
        elif isinstance(value, list):
            # Array - use JSON_EXTRACT
            return f"JSON_EXTRACT({col}, '{path}')"
        elif isinstance(value, bool):
            # Boolean
            return f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS BOOL)"
        elif isinstance(value, int):
            # Integer
            return f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS INT64)"
        elif isinstance(value, float):
            # Float
            return f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS FLOAT64)"
        else:
            # String or other
            return f"JSON_EXTRACT_SCALAR({col}, '{path}')"

    def _struct_from_schema(self, col: str, schema: str, prefix: str = "") -> str:
        """
        Very simple recursive schema parser.
        
        Args:
            col: Column name containing JSON strings
            schema: BigQuery STRUCT schema string
            prefix: JSON path prefix for nested objects
            
        Returns:
            str: Generated BigQuery STRUCT SQL
        """
        schema = schema.strip()
        if not schema.upper().startswith("STRUCT<"):
            return f"JSON_EXTRACT({col}, '$.{prefix}')"

        inner = schema[7:-1].strip()
        fields = []
        depth, buf, fname = 0, "", ""
        
        for c in inner + ",":
            if c == "<":
                depth += 1
                buf += c
            elif c == ">":
                depth -= 1
                buf += c
            elif c == ":" and depth == 0 and not fname:
                fname, buf = buf.strip(), ""
            elif c == "," and depth == 0:
                ftype = buf.strip()
                buf = ""
                path = f"$.{prefix}.{fname}" if prefix else f"$.{fname}"
                
                if ftype.upper().startswith("STRUCT<"):
                    nested = self._struct_from_schema(col, ftype, f"{prefix}.{fname}" if prefix else fname)
                    fields.append(f"{nested} AS `{fname}`")
                elif ftype.upper().startswith("ARRAY<"):
                    fields.append(f"JSON_EXTRACT({col}, '{path}') AS `{fname}`")
                elif ftype.upper().startswith(("INT", "INT64")):
                    fields.append(f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS INT64) AS `{fname}`")
                elif ftype.upper().startswith(("FLOAT", "FLOAT64")):
                    fields.append(f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS FLOAT64) AS `{fname}`")
                elif ftype.upper().startswith(("BOOL", "BOOLEAN")):
                    fields.append(f"CAST(JSON_EXTRACT_SCALAR({col}, '{path}') AS BOOL) AS `{fname}`")
                else:
                    fields.append(f"JSON_EXTRACT_SCALAR({col}, '{path}') AS `{fname}`")
                fname = ""
            else:
                buf += c
        
        return "STRUCT(\n  " + ",\n  ".join(fields) + "\n)"
    

    def apply(self, props: JSONParseProperties) -> str:
        """
        Apply the JSONParse transformation and return the macro call.
        
        This method generates the final macro call with all the necessary parameters,
        including the generated SQL for native JSON parsing. The macro will add a 
        single column with the parsed JSON result, or fall back to SELECT * if parsing fails.
        
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
        """
        Load properties from macro parameters.
        
        Args:
            properties: The macro properties
            
        Returns:
            PropertiesType: The loaded properties
        """
        parametersMap = self.convertToParameterMap(properties.parameters)
        print(f"The name of the parametersMap is {parametersMap}")
        return JSONParse.JSONParseProperties(
            relation_name=parametersMap.get("relation_name"),
            columnName=parametersMap.get("columnName"),
            parsingMethod=parametersMap.get("parsingMethod"),
            sampleRecord=parametersMap.get("sampleRecord"),
            sampleSchema=parametersMap.get("sampleSchema"),
            generated_sql=parametersMap.get("generated_sql"),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        """
        Unload properties to macro parameters.
        
        Args:
            properties: The component properties
            
        Returns:
            MacroProperties: The macro properties
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
        Update input port slug based on upstream connections.
        
        Args:
            component: The component to update
            context: The SQL context
            
        Returns:
            Component: Updated component
        """
        relation_name = self.get_relation_names(component, context)
        return replace(
            component,
            properties=replace(component.properties, relation_name=relation_name),
        )
