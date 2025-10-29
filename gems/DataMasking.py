from dataclasses import dataclass

import dataclasses
import json
from collections import defaultdict
from typing import Any
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *

from pyspark.sql import *
from pyspark.sql.functions import *


class DataMasking(MacroSpec):
    name: str = "DataMasking"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake,
        # ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]

    @dataclass(frozen=True)
    class DataMaskingProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        column_names: List[str] = field(default_factory=list)
        masking_method: str = ""
        upper_char_substitute: str = ""
        lower_char_substitute: str = ""
        digit_char_substitute: str = ""
        other_char_substitute: str = ""
        sha2_bit_length: str = ""
        prefix_suffix_option: str = "Prefix"
        prefix_suffix_added: str = ""
        combined_hash_column_name: str = ""
        masked_column_add_method: str = "inplace_substitute"

    def dialog(self) -> Dialog:
        mask_condition = Condition().ifEqual(
            PropExpr("component.properties.masking_method"), StringExpr("mask")
        )

        hash_condition = Condition().ifEqual(
            PropExpr("component.properties.masking_method"), StringExpr("hash")
        )

        not_hash_condition = Condition().ifNotEqual(
            PropExpr("component.properties.masking_method"), StringExpr("hash")
        )

        sha2_condition = Condition().ifEqual(
            PropExpr("component.properties.masking_method"), StringExpr("sha2")
        )

        mask_params_ui = StackLayout(height="100%").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    TextBox("Uppercase character replacement")
                    .bindProperty("upper_char_substitute")
                    .bindPlaceholder(
                        "Default value is 'X'. Specify NULL to retain original character"
                    )
                )
                .addElement(
                    TextBox("Lowercase character replacement")
                    .bindProperty("lower_char_substitute")
                    .bindPlaceholder(
                        "Default value is 'x'. Specify NULL to retain original character"
                    )
                )
            )
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    TextBox("Digit replacement")
                    .bindProperty("digit_char_substitute")
                    .bindPlaceholder(
                        "Default value is 'n'. Specify NULL to retain original character"
                    )
                )
                .addElement(
                    TextBox("Special character replacement")
                    .bindProperty("other_char_substitute")
                    .bindPlaceholder(
                        "Default value is NULL. Specify NULL to retain original character."
                    )
                )
            )
        )

        selectBox_nonHash = (
            RadioGroup("")
            .addOption(
                "Substitute the new columns in place",
                "inplace_substitute",
                description=(
                    "This option will substitute the original columns to have masked value with same name"
                ),
            )
            .addOption(
                "Add new columns with a prefix/suffix attached",
                "prefix_suffix_substitute",
                description="This option will keep the original columns intact and add new columns with added prefix/suffix to respective columns",
            )
            .setOptionType("button")
            .setVariant("medium")
            .setButtonStyle("solid")
            .bindProperty("masked_column_add_method")
        )
        selectBox_Hash = (
            RadioGroup("")
            .addOption(
                "Substitute the new columns in place",
                "inplace_substitute",
                description=(
                    "This option will substitute the original columns to have masked value with same name"
                ),
            )
            .addOption(
                "Add new columns with a prefix/suffix attached",
                "prefix_suffix_substitute",
                description="This option will keep the original columns intact and add new columns with added prefix/suffix to respective columns",
            )
            .addOption(
                "Apply a single hash to all the selected columns at once",
                "combinedHash_substitute",
                description="This option will apply a single hash to all the selected columns at once",
            )
            .setOptionType("button")
            .setVariant("medium")
            .setButtonStyle("solid")
            .bindProperty("masked_column_add_method")
        )

        sha2_params_ui = StackLayout(
            gap="1rem", height="100%", direction="vertical", width="100%"
        ).addElement(
            Condition()
                .ifEqual(PropExpr("$.sql.metainfo.providerType"), StringExpr("ProphecyManaged"))
                .then(
                    SelectBox("Select the bit length")
                    .bindProperty("sha2_bit_length")
                    .withDefault("256")
                    .addOption("256", "256")                
                )
                .otherwise(
                    SelectBox("Select the bit length")
                    .bindProperty("sha2_bit_length")
                    .withDefault("")
                    .addOption("224", "224")
                    .addOption("256", "256")
                    .addOption("384", "384")
                    .addOption("512", "512")                    
                )
        )

        dialog = Dialog("masking_dialog_box").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Select masking columns"))
                            .addElement(
                                SchemaColumnsDropdown("", appearance="minimal")
                                .withMultipleSelection()
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("column_names")
                            )
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Masking configuration"))
                            .addElement(
                                Condition()
                                    .ifEqual(PropExpr("$.sql.metainfo.providerType"), StringExpr("ProphecyManaged"))
                                    .then(
                                        SelectBox("Select masking strategy")
                                        .bindProperty("masking_method")
                                        .withStyle({"width": "100%"})
                                        .withDefault("")
                                        .addOption("hash", "hash")
                                        .addOption("sha", "sha")
                                        .addOption("sha2", "sha2")
                                        .addOption("md5", "md5")
                                    )
                                    .otherwise(
                                        SelectBox("Select masking strategy")
                                        .bindProperty("masking_method")
                                        .withStyle({"width": "100%"})
                                        .withDefault("")
                                        .addOption("hash", "hash")
                                        .addOption("sha", "sha")
                                        .addOption("sha2", "sha2")
                                        .addOption("md5", "md5")
                                        .addOption("mask", "mask")
                                        .addOption("crc32", "crc32")                                        
                                    )
                            )
                            .addElement(mask_condition.then(mask_params_ui))
                            .addElement(sha2_condition.then(sha2_params_ui))
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Masked column options"))
                            .addElement(
                                hash_condition.then(selectBox_Hash).otherwise(
                                    selectBox_nonHash
                                )
                            )
                            .addElement(
                                Condition()
                                .ifEqual(
                                    PropExpr(
                                        "component.properties.masked_column_add_method"
                                    ),
                                    StringExpr("prefix_suffix_substitute"),
                                )
                                .then(
                                    StackLayout(height="100%").addElement(
                                        ColumnsLayout(gap="1rem", height="100%")
                                        .addColumn(
                                            SelectBox("Select type")
                                            .addOption("prefix", "Prefix")
                                            .addOption("suffix", "Suffix")
                                            .bindProperty("prefix_suffix_option"),
                                            "50%",
                                        )
                                        .addColumn(
                                            TextBox("Enter the value")
                                            .bindPlaceholder("Example: new_")
                                            .bindProperty("prefix_suffix_added"),
                                            "50%",
                                        )
                                    )
                                )
                            )
                            .addElement(
                                Condition()
                                .ifEqual(
                                    PropExpr(
                                        "component.properties.masked_column_add_method"
                                    ),
                                    StringExpr("combinedHash_substitute"),
                                )
                                .then(
                                    TextBox("new column name for combined hash")
                                    .bindPlaceholder("")
                                    .bindProperty("combined_hash_column_name")
                                )
                            )
                        )
                    )
                )
            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(DataMasking, self).validate(context, component)

        schema_columns = []
        schema_js = json.loads(component.properties.schema)
        for js in schema_js:
            schema_columns.append(js["name"].lower())

        if len(component.properties.column_names) == 0:
            diagnostics.append(
                Diagnostic(
                    "component.properties.column_names",
                    f"Select atleast one column to apply masking on",
                    SeverityLevelEnum.Error,
                )
            )
        elif len(component.properties.column_names) > 0:
            missingKeyColumns = [
                col
                for col in component.properties.column_names
                if col.lower() not in schema_columns
            ]
            if missingKeyColumns:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.column_names",
                        f"Selected columns {missingKeyColumns} are not present in input schema.",
                        SeverityLevelEnum.Error,
                    )
                )
        if component.properties.masking_method == "":
            diagnostics.append(
                Diagnostic(
                    "component.properties.masking_method",
                    f"Select one masking method",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.masked_column_add_method == "prefix_suffix_substitute":
            if component.properties.prefix_suffix_option == "":
                diagnostics.append(
                    Diagnostic(
                        "component.properties.prefix_suffix_option",
                        f"Select one option out of Prefix/Suffix for new column names",
                        SeverityLevelEnum.Error,
                    )
                )
            if component.properties.prefix_suffix_added == "":
                diagnostics.append(
                    Diagnostic(
                        "component.properties.prefix_suffix_option",
                        f"Enter the prefix/suffix value to be added to new column",
                        SeverityLevelEnum.Error,
                    )
                )
        if component.properties.masked_column_add_method == "combinedHash_substitute":
            if component.properties.combined_hash_column_name == "":
                diagnostics.append(
                    Diagnostic(
                        "component.properties.combined_hash_column_name",
                        f"Enter the new column name for combined hash",
                        SeverityLevelEnum.Error,
                    )
                )
        if (
            component.properties.masking_method == "sha2"
            and component.properties.sha2_bit_length == ""
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.masking_method",
                    f"bit length for sha2 masking cannot be empty.",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.masking_method == "mask" and (
            (component.properties.upper_char_substitute).upper() != "NULL"
            and len(component.properties.upper_char_substitute) > 1
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.upper_char_substitute",
                    f"length for upperChar substitute key cannot be greater than 1",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.masking_method == "mask" and (
            (component.properties.lower_char_substitute).upper() != "NULL"
            and len(component.properties.lower_char_substitute) > 1
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.lower_char_substitute",
                    f"length for lowerChar substitute key cannot be greater than 1",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.masking_method == "mask" and (
            (component.properties.digit_char_substitute).upper() != "NULL"
            and len(component.properties.digit_char_substitute) > 1
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.digit_char_substitute",
                    f"length for digitChar substitute key cannot be greater than 1",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.masking_method == "mask" and (
            (component.properties.other_char_substitute).upper() != "NULL"
            and len(component.properties.other_char_substitute) > 1
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.other_char_substitute",
                    f"length for otherChar substitute key cannot be greater than 1",
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

    def apply(self, props: DataMaskingProperties) -> str:
        # Generate the actual macro call given the component's state
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        resolved_macro_name = f"{self.projectName}.{self.name}"
        schema_columns = [js["name"] for js in json.loads(props.schema)]
        remaining_columns = ", ".join(
            list(set(schema_columns) - set(props.column_names))
        )

        def safe_str(val):
            if val is None or val == "":
                return "''"
            if isinstance(val, list):
                return str(val)
            return f"'{val}'"

        arguments = [
            safe_str(table_name),
            safe_str(props.column_names),
            safe_str(remaining_columns),
            safe_str(props.masking_method),
            safe_str(
                props.upper_char_substitute
                if props.upper_char_substitute.upper() != "NULL"
                else props.upper_char_substitute.upper()
            ),
            safe_str(
                props.lower_char_substitute
                if props.lower_char_substitute.upper() != "NULL"
                else props.lower_char_substitute.upper()
            ),
            safe_str(
                props.digit_char_substitute
                if props.digit_char_substitute.upper() != "NULL"
                else props.digit_char_substitute.upper()
            ),
            safe_str(
                props.other_char_substitute
                if props.other_char_substitute.upper() != "NULL"
                else props.other_char_substitute.upper()
            ),
            safe_str(props.sha2_bit_length),
            safe_str(props.masked_column_add_method),
            safe_str(props.prefix_suffix_option),
            safe_str(props.prefix_suffix_added),
            safe_str(props.combined_hash_column_name),
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return DataMasking.DataMaskingProperties(
            relation_name=parametersMap.get("relation_name"),
            schema=parametersMap.get("schema"),
            column_names=json.loads(
                parametersMap.get("column_names").replace("'", '"')
            ),
            masking_method=parametersMap.get("masking_method"),
            upper_char_substitute=parametersMap.get("upper_char_substitute"),
            lower_char_substitute=parametersMap.get("lower_char_substitute"),
            digit_char_substitute=parametersMap.get("digit_char_substitute"),
            other_char_substitute=parametersMap.get("other_char_substitute"),
            sha2_bit_length=parametersMap.get("sha2_bit_length"),
            masked_column_add_method=parametersMap.get("masked_column_add_method"),
            prefix_suffix_option=parametersMap.get("prefix_suffix_option"),
            prefix_suffix_added=parametersMap.get("prefix_suffix_added"),
            combined_hash_column_name=parametersMap.get("combined_hash_column_name"),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("column_names", json.dumps(properties.column_names)),
                MacroParameter("masking_method", str(properties.masking_method)),
                MacroParameter(
                    "upper_char_substitute", str(properties.upper_char_substitute)
                ),
                MacroParameter(
                    "lower_char_substitute", str(properties.lower_char_substitute)
                ),
                MacroParameter(
                    "digit_char_substitute", str(properties.digit_char_substitute)
                ),
                MacroParameter(
                    "other_char_substitute", str(properties.other_char_substitute)
                ),
                MacroParameter("sha2_bit_length", str(properties.sha2_bit_length)),
                MacroParameter(
                    "masked_column_add_method", str(properties.masked_column_add_method)
                ),
                MacroParameter(
                    "prefix_suffix_option", str(properties.prefix_suffix_option)
                ),
                MacroParameter(
                    "prefix_suffix_added", str(properties.prefix_suffix_added)
                ),
                MacroParameter(
                    "combined_hash_column_name",
                    str(properties.combined_hash_column_name),
                ),
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
        Apply masking transformations to selected columns.
        Supports hash, sha, sha2, md5, mask, and crc32 methods.
        """
        if not self.props.column_names:
            return in0
        
        method = self.props.masking_method
        all_cols = set[Any](in0.columns)
        remaining_cols = list(all_cols - set(self.props.column_names))
        
        select_exprs = []
        
        # Add remaining columns
        for col_name in in0.columns:
            if col_name in remaining_cols:
                select_exprs.append(col(col_name))
        
        # Apply masking to selected columns
        if self.props.masked_column_add_method == "combinedHash_substitute" and method == "hash":
            # Combined hash for multiple columns
            concat_cols = [col(c) for c in self.props.column_names]
            hash_expr = hash(*concat_cols)
            select_exprs.append(hash_expr.alias(self.props.combined_hash_column_name))
        else:
            # Individual column masking
            for col_name in self.props.column_names:
                col_expr = col(col_name)
                new_col_name = col_name
                
                if self.props.masked_column_add_method == "prefix_suffix_substitute":
                    if self.props.prefix_suffix_option == "Prefix":
                        new_col_name = f"{self.props.prefix_suffix_added}{col_name}"
                    else:
                        new_col_name = f"{col_name}{self.props.prefix_suffix_added}"
                
                # Apply masking method
                if method == "hash":
                    masked_expr = hash(col_expr)
                elif method == "md5":
                    masked_expr = md5(col_expr.cast("string"))
                elif method == "sha":
                    masked_expr = sha1(col_expr.cast("string"))
                elif method == "sha2":
                    bit_length = int(self.props.sha2_bit_length) if self.props.sha2_bit_length else 256
                    masked_expr = sha2(col_expr.cast("string"), bit_length)
                elif method == "crc32":
                    masked_expr = crc32(col_expr.cast("string"))
                elif method == "mask":
                    # Mask function with character replacement
                    upper_char = self.props.upper_char_substitute if self.props.upper_char_substitute.upper() != "NULL" else "X"
                    lower_char = self.props.lower_char_substitute if self.props.lower_char_substitute.upper() != "NULL" else "x"
                    digit_char = self.props.digit_char_substitute if self.props.digit_char_substitute.upper() != "NULL" else "n"
                    other_char = self.props.other_char_substitute if self.props.other_char_substitute.upper() != "NULL" else None
                    
                    # Apply regex replacements for masking
                    masked_expr = col_expr.cast("string")
                    if upper_char and upper_char != "NULL":
                        masked_expr = regexp_replace(masked_expr, r"[A-Z]", upper_char)
                    if lower_char and lower_char != "NULL":
                        masked_expr = regexp_replace(masked_expr, r"[a-z]", lower_char)
                    if digit_char and digit_char != "NULL":
                        masked_expr = regexp_replace(masked_expr, r"\d", digit_char)
                    if other_char and other_char != "NULL":
                        masked_expr = regexp_replace(masked_expr, r"[^a-zA-Z0-9]", other_char)
                else:
                    masked_expr = col_expr
                
                select_exprs.append(masked_expr.alias(new_col_name))
        
        return in0.select(*select_exprs)
