import dataclasses
import datetime as dt
import json

import re
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


class DataCleansing(MacroSpec):
    name: str = "DataCleansing"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]
    dependsOnUpstreamSchema: bool = True

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
        modifyCase: str = "keepOriginal"
        replaceNullDateFields: bool = False
        replaceNullDateWith: str = "1970-01-01"
        replaceNullTimeFields: bool = False
        replaceNullTimeWith: str = "1970-01-01 00:00:00.0"

    def get_relation_names(self, component: Component, context: SqlContext):
        relation_name = []
        for input_port in component.ports.inputs:
            if input_port.slug and not re.match(r'^in\d+$', input_port.slug):
                relation_name.append(input_port.slug)
            else:
                upstream_label = ""
                for connection in context.graph.connections:
                    if connection.targetPort == input_port.id:
                        upstream_node = context.graph.nodes.get(connection.source)
                        if upstream_node is not None and upstream_node.label is not None:
                            upstream_label = upstream_node.label
                relation_name.append(upstream_label)
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
                Checkbox("Leading and trailing whitespace").bindProperty(
                    "trimWhiteSpace"
                )
            )
            .addElement(
                Checkbox("Tabs, line breaks and duplicate whitespace").bindProperty(
                    "removeTabsLineBreaksAndDuplicateWhitespace"
                )
            )
            .addElement(Checkbox("All whitespace").bindProperty("allWhiteSpace"))
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
                    Checkbox("Trim whitespace").bindProperty("trimWhiteSpace"), "1fr"
                )
                .addColumn(
                    Checkbox(
                        "Remove tabs, line breaks and duplicate whitespace"
                    ).bindProperty("removeTabsLineBreaksAndDuplicateWhitespace"),
                    "2fr",
                )
                .addColumn(
                    Checkbox("Remove all whitespace").bindProperty("allWhiteSpace"),
                    "1fr",
                )
            )
            .addElement(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                    Checkbox("Remove letters").bindProperty("cleanLetters"), "1fr"
                )
                .addColumn(
                    Checkbox("Remove punctuation").bindProperty("cleanPunctuations"),
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

        if len(component.properties.columnNames) > 0 and component.properties.schema:
            schema_cols_lower = set(col["name"].lower() for col in json.loads(component.properties.schema))
            
            missingKeyColumns = [
                col
                for col in component.properties.columnNames
                if col.lower() not in schema_cols_lower
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
        schema = (json.loads(newState.ports.inputs[0].schema) if isinstance(newState.ports.inputs[0].schema, str) else (newState.ports.inputs[0].schema or {}))
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

        # generate the actual macro call given the component's
        resolved_macro_name = f"{self.projectName}.{self.name}"

        def safe_str(val):
            # Escape backslashes and single quotes so values containing
            # apostrophes remain a valid Jinja string literal in the
            # generated macro call.
            escaped = str(val).replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"

        arguments = [
            str(props.relation_name),
            props.schema,
            safe_str(props.modifyCase),
            str(props.columnNames),
            str(props.replaceNullTextFields).lower(),
            safe_str(props.replaceNullTextWith),
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
            safe_str(props.replaceNullDateWith),
            str(props.replaceNullTimeFields).lower(),
            safe_str(props.replaceNullTimeWith),
        ]

        params = ",".join([param for param in arguments])
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # Load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)

        def _load_json_value(raw: str, default):
            raw = raw or ""
            if raw == "":
                return default
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return json.loads(raw.replace("'", '"'))

        def _unquote(raw: str, default: str = "") -> str:
            # Strip the surrounding single quotes emitted by apply() and
            # reverse its backslash/apostrophe escaping.
            if raw is None or raw == "":
                return default
            if len(raw) >= 2 and raw.startswith("'") and raw.endswith("'"):
                raw = raw[1:-1]
            return raw.replace("\\'", "'").replace("\\\\", "\\")

        def _bool(name: str) -> bool:
            return (parametersMap.get(name) or "").lower() == "true"

        # Keep integral values as int so the generated code round-trips
        # without rewriting e.g. 0 as 0.0.
        numeric_with = float(_unquote(parametersMap.get("replaceNullNumericWith"), "0") or "0")
        if numeric_with.is_integer():
            numeric_with = int(numeric_with)

        return DataCleansing.DataCleansingProperties(
            relation_name=_load_json_value(parametersMap.get("relation_name"), []),
            schema=parametersMap.get("schema") or "",
            modifyCase=_unquote(parametersMap.get("modifyCase"), "keepOriginal") or "keepOriginal",
            columnNames=_load_json_value(parametersMap.get("columnNames"), []),
            replaceNullTextFields=_bool("replaceNullTextFields"),
            replaceNullTextWith=_unquote(parametersMap.get("replaceNullTextWith"), "NA"),
            replaceNullForNumericFields=_bool("replaceNullForNumericFields"),
            replaceNullNumericWith=numeric_with,
            trimWhiteSpace=_bool("trimWhiteSpace"),
            removeTabsLineBreaksAndDuplicateWhitespace=_bool(
                "removeTabsLineBreaksAndDuplicateWhitespace"
            ),
            allWhiteSpace=_bool("allWhiteSpace"),
            cleanLetters=_bool("cleanLetters"),
            cleanPunctuations=_bool("cleanPunctuations"),
            cleanNumbers=_bool("cleanNumbers"),
            removeRowNullAllCols=_bool("removeRowNullAllCols"),
            replaceNullDateFields=_bool("replaceNullDateFields"),
            replaceNullDateWith=_unquote(parametersMap.get("replaceNullDateWith"), "1970-01-01"),
            replaceNullTimeFields=_bool("replaceNullTimeFields"),
            replaceNullTimeWith=_unquote(
                parametersMap.get("replaceNullTimeWith"), "1970-01-01 00:00:00.0"
            ),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
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
        schema = (json.loads(component.ports.inputs[0].schema) if isinstance(component.ports.inputs[0].schema, str) else (component.ports.inputs[0].schema or {}))
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
        remove_row_null_all_cols = self.props.removeRowNullAllCols
        cleansing_columns = self.props.columnNames
        replace_null_text_fields = self.props.replaceNullTextFields
        replace_null_text_with = self.props.replaceNullTextWith
        replace_null_numeric_fields = self.props.replaceNullForNumericFields
        replace_null_numeric_with = self.props.replaceNullNumericWith
        trim_whitespace = self.props.trimWhiteSpace
        remove_tabs_linebreaks = self.props.removeTabsLineBreaksAndDuplicateWhitespace
        all_whitespace = self.props.allWhiteSpace
        clean_letters = self.props.cleanLetters
        clean_punctuations = self.props.cleanPunctuations
        clean_numbers = self.props.cleanNumbers
        modify_case = self.props.modifyCase
        replace_null_date_fields = self.props.replaceNullDateFields
        replace_null_date_with = self.props.replaceNullDateWith
        replace_null_time_fields = self.props.replaceNullTimeFields
        replace_null_time_with = self.props.replaceNullTimeWith

        if remove_row_null_all_cols:
            result_df = in0.na.drop(how="all")
        else:
            result_df = in0

        all_expressions = []
        
        for col_name in result_df.columns:
            if col_name in cleansing_columns:
                col_type = result_df.schema[col_name].dataType
                if isinstance(col_type, StringType):
                    col_expr = col(col_name)
                    if replace_null_text_fields:
                        col_expr = coalesce(col_expr, lit(replace_null_text_with))

                    if trim_whitespace:
                        col_expr = trim(col_expr)

                    if remove_tabs_linebreaks:
                        col_expr = regexp_replace(col_expr, r'\s+', ' ')

                    if all_whitespace:
                        col_expr = regexp_replace(col_expr, r'\s+', '')

                    if clean_letters:
                        col_expr = regexp_replace(col_expr, r'[A-Za-z]', '')

                    if clean_punctuations:
                        col_expr = regexp_replace(col_expr, r'[^\w\s]', '')

                    if clean_numbers:
                        col_expr = regexp_replace(col_expr, r'\d+', '')

                    if modify_case == "makeLowercase":
                        col_expr = lower(col_expr)
                    elif modify_case == "makeUppercase":
                        col_expr = upper(col_expr)
                    elif modify_case == "makeTitlecase":
                        col_expr = initcap(col_expr)

                    all_expressions.append(col_expr.alias(col_name))

                elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType, ByteType, DecimalType)):
                    col_expr = col(col_name)

                    if replace_null_numeric_fields:
                        col_expr = coalesce(col_expr, lit(replace_null_numeric_with).cast(col_type))
                    
                    all_expressions.append(col_expr.alias(col_name))
                    
                elif isinstance(col_type, DateType):
                    col_expr = col(col_name)

                    if replace_null_date_fields:
                        col_expr = coalesce(col_expr, to_date(lit(replace_null_date_with))).cast(DateType())
                    
                    all_expressions.append(col_expr.alias(col_name))
                    
                elif isinstance(col_type, TimestampType):
                    col_expr = col(col_name)

                    if replace_null_time_fields:
                        col_expr = coalesce(col_expr, to_timestamp(lit(replace_null_time_with))).cast(TimestampType())
                    
                    all_expressions.append(col_expr.alias(col_name))
                else:
                    all_expressions.append(col(col_name))
            else:
                all_expressions.append(col(col_name))
        
        result_df = result_df.select(*all_expressions)

        return result_df
