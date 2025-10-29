import dataclasses
import datetime as dt
import json

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *

from pyspark.sql import *
from pyspark.sql.functions import *


class DataCleansing(MacroSpec):
    name: str = "DataCleansing"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]

    @dataclass(frozen=True)
    class DataCleansingProperties(MacroProperties):
        # properties for the component with default values
        schema: str = ""
        relation_name: List[str] = field(default_factory=list)

        # null check operations
        removeRowNullAllCols: bool = False

        # clean checks
        columnNames: List[str] = field(default_factory=list)
        replaceNullTextFields: bool = False
        replaceNullTextWith: str = "NA"
        replaceNullForNumericFields: bool = False
        replaceNullNumericWith: int = 0
        trimWhiteSpace: bool = False
        removeTabsLineBreaksAndDuplicateWhitespace: bool = False
        allWhiteSpace: bool = False
        cleanLetters: bool = False
        cleanPunctuations: bool = False
        cleanNumbers: bool = False
        modifyCase: str = "Keep original"
        replaceNullDateFields: bool = False
        replaceNullDateWith: str = "1970-01-01"
        replaceNullTimeFields: bool = False
        replaceNullTimeWith: str = "1970-01-01 00:00:00.0"

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
        nullOpCheckBox = ColumnsLayout(gap="1rem", height="100%").addColumn(
            StackLayout(height="100%").addElement(
                Checkbox("Remove rows with null in every column").bindProperty(
                    "removeRowNullAllCols"
                )
            )
        )

        selectCol = (
            SchemaColumnsDropdown("", appearance="minimal")
            .withMultipleSelection()
            .bindSchema("component.ports.inputs[0].schema")
            .bindProperty("columnNames")
        )

        options = ColumnsLayout(gap="1rem", height="100%").addColumn(
            StackLayout(height="100%")
            .addElement(
                Checkbox("Leading and trailing whitespaces").bindProperty(
                    "trimWhiteSpace"
                )
            )
            .addElement(
                Checkbox("Tabs, line breaks and duplicate whitespaces").bindProperty(
                    "removeTabsLineBreaksAndDuplicateWhitespace"
                )
            )
            .addElement(Checkbox("All whitespaces").bindProperty("allWhiteSpace"))
            .addElement(Checkbox("Letters").bindProperty("cleanLetters"))
            .addElement(Checkbox("Numbers").bindProperty("cleanNumbers"))
            .addElement(Checkbox("Punctuations").bindProperty("cleanPunctuations"))
            .addElement(NativeText("Modify case"))
            .addElement(
                SelectBox("")
                .addOption("Keep original", "keepOriginal")
                .addOption("lowercase", "makeLowercase")
                .addOption("UPPERCASE", "makeUppercase")
                .addOption("Title Case", "makeTitlecase")
                .bindProperty("modifyCase")
            )
        )

        # TBD: Need to Remove
        options_to_remove = (
            StackLayout(gap="2em")
            .addElement(NativeText("Remove unwanted characters"))
            .addElement(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                    Checkbox("Trim whitespaces").bindProperty("trimWhiteSpace"), "1fr"
                )
                .addColumn(
                    Checkbox(
                        "Remove tabs, line breaks and duplicate whitespace"
                    ).bindProperty("removeTabsLineBreaksAndDuplicateWhitespace"),
                    "2fr",
                )
                .addColumn(
                    Checkbox("Remove all whitespaces").bindProperty("allWhiteSpace"),
                    "1fr",
                )
            )
            .addElement(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                    Checkbox("Remove letters").bindProperty("cleanLetters"), "1fr"
                )
                .addColumn(
                    Checkbox("Remove punctuations").bindProperty("cleanPunctuations"),
                    "2fr",
                )
                .addColumn(
                    Checkbox("Remove numbers").bindProperty("cleanNumbers"), "1fr"
                )
            )
        )

        return Dialog("DataCleansing").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout()
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(
                                TitleElement("Remove nulls from entire dataset")
                            )
                            .addElement(nullOpCheckBox)
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Select columns to clean"))
                            .addElement(selectCol)
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Clean selected columns"))
                            .addElement(NativeText("Replace null values in column"))
                            .addElement(
                                Checkbox("For string columns").bindProperty(
                                    "replaceNullTextFields"
                                )
                            )
                            .addElement(
                                Condition()
                                .ifEqual(
                                    PropExpr(
                                        "component.properties.replaceNullTextFields"
                                    ),
                                    BooleanExpr(True),
                                )
                                .then(
                                    TextBox(
                                        "Value to replace String/Text field",
                                        placeholder="NA",
                                    ).bindProperty("replaceNullTextWith"),
                                )
                            )
                            .addElement(
                                Checkbox("For numeric columns").bindProperty(
                                    "replaceNullForNumericFields"
                                )
                            )
                            .addElement(
                                Condition()
                                .ifEqual(
                                    PropExpr(
                                        "component.properties.replaceNullForNumericFields"
                                    ),
                                    BooleanExpr(True),
                                )
                                .then(
                                    NumberBox(
                                        "Value to replace Numeric field",
                                        placeholder="0",
                                    )
                                    .withMin(-9999999999999999)
                                    .bindProperty("replaceNullNumericWith"),
                                )
                            )
                            .addElement(
                                Checkbox("For Date columns").bindProperty(
                                    "replaceNullDateFields"
                                )
                            )
                            .addElement(
                                Condition()
                                .ifEqual(
                                    PropExpr(
                                        "component.properties.replaceNullDateFields"
                                    ),
                                    BooleanExpr(True),
                                )
                                .then(
                                    TextBox(
                                        "Value to replace Date field in YYYY-MM-DD format",
                                        placeholder="1970-01-01",
                                    ).bindProperty("replaceNullDateWith"),
                                )
                            )
                            .addElement(
                                Checkbox("For Time columns").bindProperty(
                                    "replaceNullTimeFields"
                                )
                            )
                            .addElement(
                                Condition()
                                .ifEqual(
                                    PropExpr(
                                        "component.properties.replaceNullTimeFields"
                                    ),
                                    BooleanExpr(True),
                                )
                                .then(
                                    TextBox(
                                        "Value to replace Time field in YYYY-MM-DD HH:MM:SS.s format",
                                        placeholder="1970-01-01 00:00:00.0",
                                    ).bindProperty("replaceNullTimeWith"),
                                )
                            )
                            .addElement(NativeText("Remove unwanted characters"))
                            .addElement(options)
                        )
                    )
                )
            )
        )

    def is_valid_date(self, date_string, str_format) -> bool:
        try:
            dt.datetime.strptime(date_string, str_format)
            return True
        except ValueError:
            return False

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(DataCleansing, self).validate(context, component)

        if len(component.properties.columnNames) > 0:
            missingKeyColumns = [
                col
                for col in component.properties.columnNames
                if col not in component.properties.schema
            ]
            if missingKeyColumns:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.columnNames",
                        f"Selected columns {missingKeyColumns} are not present in input schema.",
                        SeverityLevelEnum.Error,
                    )
                )

        if component.properties.replaceNullDateFields and not self.is_valid_date(
            component.properties.replaceNullDateWith, "%Y-%m-%d"
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.replaceNullDateFields",
                    "Enter a valid date in YYYY-MM-DD format (e.g., 1970-01-01).",
                    SeverityLevelEnum.Error,
                )
            )

        if component.properties.replaceNullTimeFields and not self.is_valid_date(
            component.properties.replaceNullTimeWith, "%Y-%m-%d %H:%M:%S.%f"
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.replaceNullTimeFields",
                    "Enter a valid timestamp in YYYY-MM-DD HH:MM:SS.sss format (e.g., 1970-01-01 00:00:00.000).",
                    SeverityLevelEnum.Error,
                )
            )

        return diagnostics

    def onChange(
        self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [
            {"name": field["name"], "dataType": field["dataType"]["type"]}
            for field in schema["fields"]
        ]
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: DataCleansingProperties) -> str:
        # Get the table name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # generate the actual macro call given the component's
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + table_name + "'",
            props.schema,
            "'" + props.modifyCase + "'",
            str(props.columnNames),
            str(props.replaceNullTextFields).lower(),
            "'" + str(props.replaceNullTextWith) + "'",
            str(props.replaceNullForNumericFields).lower(),
            str(props.replaceNullNumericWith),
            str(props.trimWhiteSpace).lower(),
            str(props.removeTabsLineBreaksAndDuplicateWhitespace).lower(),
            str(props.allWhiteSpace).lower(),
            str(props.cleanLetters).lower(),
            str(props.cleanPunctuations).lower(),
            str(props.cleanNumbers).lower(),
            str(props.removeRowNullAllCols).lower(),
            str(props.replaceNullDateFields).lower(),
            "'" + str(props.replaceNullDateWith) + "'",
            str(props.replaceNullTimeFields).lower(),
            "'" + str(props.replaceNullTimeWith) + "'",
        ]

        params = ",".join([param for param in arguments])
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # Load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        print("parametersMapisHere")
        print(parametersMap)
        return DataCleansing.DataCleansingProperties(
            relation_name=parametersMap.get("relation_name"),
            schema=parametersMap.get("schema"),
            modifyCase=parametersMap.get("modifyCase"),
            columnNames=json.loads(parametersMap.get("columnNames").replace("'", '"')),
            replaceNullTextFields=parametersMap.get("replaceNullTextFields").lower()
            == "true",
            replaceNullTextWith=parametersMap.get("replaceNullTextWith")[1:-1],
            replaceNullForNumericFields=parametersMap.get(
                "replaceNullForNumericFields"
            ).lower()
            == "true",
            replaceNullNumericWith=float(parametersMap.get("replaceNullNumericWith")),
            trimWhiteSpace=parametersMap.get("trimWhiteSpace").lower() == "true",
            removeTabsLineBreaksAndDuplicateWhitespace=parametersMap.get(
                "removeTabsLineBreaksAndDuplicateWhitespace"
            ).lower()
            == "true",
            allWhiteSpace=parametersMap.get("allWhiteSpace").lower() == "true",
            cleanLetters=parametersMap.get("cleanLetters").lower() == "true",
            cleanPunctuations=parametersMap.get("cleanPunctuations").lower() == "true",
            cleanNumbers=parametersMap.get("cleanNumbers").lower() == "true",
            removeRowNullAllCols=parametersMap.get("removeRowNullAllCols").lower()
            == "true",
            replaceNullDateFields=parametersMap.get("replaceNullDateFields").lower()
            == "true",
            replaceNullDateWith=parametersMap.get("replaceNullDateWith")[1:-1],
            replaceNullTimeFields=parametersMap.get("replaceNullTimeFields").lower()
            == "true",
            replaceNullTimeWith=parametersMap.get("replaceNullTimeWith"),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("modifyCase", properties.modifyCase),
                MacroParameter("columnNames", json.dumps(properties.columnNames)),
                MacroParameter(
                    "replaceNullTextFields",
                    str(properties.replaceNullTextFields).lower(),
                ),
                MacroParameter("replaceNullTextWith", properties.replaceNullTextWith),
                MacroParameter(
                    "replaceNullForNumericFields",
                    str(properties.replaceNullForNumericFields).lower(),
                ),
                MacroParameter(
                    "replaceNullNumericWith", str(properties.replaceNullNumericWith)
                ),
                MacroParameter(
                    "trimWhiteSpace", str(properties.trimWhiteSpace).lower()
                ),
                MacroParameter(
                    "removeTabsLineBreaksAndDuplicateWhitespace",
                    str(properties.removeTabsLineBreaksAndDuplicateWhitespace).lower(),
                ),
                MacroParameter("allWhiteSpace", str(properties.allWhiteSpace).lower()),
                MacroParameter("cleanLetters", str(properties.cleanLetters).lower()),
                MacroParameter(
                    "cleanPunctuations", str(properties.cleanPunctuations).lower()
                ),
                MacroParameter("cleanNumbers", str(properties.cleanNumbers).lower()),
                MacroParameter(
                    "removeRowNullAllCols", str(properties.removeRowNullAllCols).lower()
                ),
                MacroParameter(
                    "replaceNullDateFields",
                    str(properties.replaceNullDateFields).lower(),
                ),
                MacroParameter("replaceNullDateWith", properties.replaceNullDateWith),
                MacroParameter(
                    "replaceNullTimeFields",
                    str(properties.replaceNullTimeFields).lower(),
                ),
                MacroParameter("replaceNullTimeWith", properties.replaceNullTimeWith),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [
            {"name": field["name"], "dataType": field["dataType"]["type"]}
            for field in schema["fields"]
        ]
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
        )
        return component.bindProperties(newProperties)

    def applyPython(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
        """
        Apply data cleansing transformations based on component properties.
        Handles null replacement, whitespace trimming, character cleaning, and case modification.
        """
        # Parse schema to get column types
        schema_json = json.loads(self.props.schema)
        col_type_map = {col["name"]: col["dataType"].lower() for col in schema_json}
        
        # Numeric types for null replacement
        numeric_types = ["bigint", "decimal", "double", "float", "integer", "int", "smallint", "tinyint"]
        
        result_df = in0
        
        # Step 1: Remove rows where all columns are null
        if self.props.removeRowNullAllCols:
            all_cols = [col for col in result_df.columns]
            # Create condition: at least one column is not null
            condition = None
            for col in all_cols:
                col_condition = col(col).isNotNull()
                if condition is None:
                    condition = col_condition
                else:
                    condition = condition | col_condition
            if condition is not None:
                result_df = result_df.filter(condition)
        
        # Early return if no columns to clean
        if not self.props.columnNames:
            return result_df
        
        # Step 2: Apply transformations to selected columns
        select_exprs = []
        remaining_cols = [col for col in result_df.columns if col not in self.props.columnNames]
        
        for col_name in result_df.columns:
            if col_name not in self.props.columnNames:
                # Keep original column
                select_exprs.append(col(col_name).alias(col_name))
                continue
            
            dtype = col_type_map.get(col_name, "").lower()
            col_expr = col(col_name)
            
            # Numeric null replacement
            if dtype in numeric_types and self.props.replaceNullForNumericFields:
                col_expr = coalesce(col_expr, lit(self.props.replaceNullNumericWith))
            
            # String-specific transformations
            if dtype == "string":
                # Replace null text fields
                if self.props.replaceNullTextFields:
                    col_expr = coalesce(col_expr, lit(self.props.replaceNullTextWith))
                
                # Trim whitespace
                if self.props.trimWhiteSpace:
                    col_expr = trim(col_expr)
                
                # Remove tabs, line breaks, and duplicate whitespace
                if self.props.removeTabsLineBreaksAndDuplicateWhitespace:
                    col_expr = regexp_replace(col_expr, r"\s+", " ")
                
                # Remove all whitespace
                if self.props.allWhiteSpace:
                    col_expr = regexp_replace(col_expr, r"\s+", "")
                
                # Remove letters
                if self.props.cleanLetters:
                    col_expr = regexp_replace(col_expr, r"[A-Za-z]+", "")
                
                # Remove punctuations (keep only alphanumeric and spaces)
                if self.props.cleanPunctuations:
                    col_expr = regexp_replace(col_expr, r"[^a-zA-Z0-9\s]", "")
                
                # Remove numbers
                if self.props.cleanNumbers:
                    col_expr = regexp_replace(col_expr, r"\d+", "")
                
                # Case modification
                if self.props.modifyCase == "makeLowercase":
                    col_expr = lower(col_expr)
                elif self.props.modifyCase == "makeUppercase":
                    col_expr = upper(col_expr)
                elif self.props.modifyCase == "makeTitlecase":
                    col_expr = initcap(col_expr)
            
            # Date null replacement
            if dtype == "date" and self.props.replaceNullDateFields:
                col_expr = coalesce(col_expr, to_date(lit(self.props.replaceNullDateWith)))
            
            # Timestamp null replacement
            if dtype == "timestamp" and self.props.replaceNullTimeFields:
                col_expr = coalesce(col_expr, to_timestamp(lit(self.props.replaceNullTimeWith)))
            
            # Cast back to original type if needed
            # Note: In PySpark, we typically don't need explicit casting unless type changes
            select_exprs.append(col_expr.alias(col_name))
        
        return result_df.select(*select_exprs)
