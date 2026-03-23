from dataclasses import dataclass
import dataclasses
from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
import json

@dataclass(frozen=True)
class ColumnExpr:
    expression: str
    format: str


@dataclass(frozen=True)
class OrderByRule:
    expression: ColumnExpr
    sortType: str = "asc"

class Tile(MacroSpec):
    name: str = "Tile"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake,
        # ProviderTypeEnum.BigQuery,
        # ProviderTypeEnum.ProphecyManaged
    ]
    dependsOnUpstreamSchema: bool = False

    @dataclass(frozen=True)
    class TileProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        tile_method: str = "equal_sum_tile"
        number_of_tiles: str = ""
        sum_column_name: str = ""
        orderByColumns: List[OrderByRule] = field(default_factory=list)
        groupby_column_names: List[str] = field(default_factory=list)
        smart_tile_column_name: str = ""
        column_output_method_smartTile: str = "no_output_column_smartTile"
        unique_value_column_name: List[str] = field(default_factory=list)
        manual_tile_column_name: str = ""
        manual_tiles_cutoff: str = ""
        donot_split_tile_column_names: List[str] = field(default_factory=list)

    def dialog(self) -> Dialog:
        select_tiling_radio_box = (RadioGroup("")
                                   .addOption("Equal Sum", "equal_sum_tile",
                                              description=("This option assigns tiles to cover a range of values where each tile has the same total of the sum field based on the sort order of the incoming records"))
                                   .addOption("Equal Records", "equal_records_tile",
                                              description="Input records are divided into the specified amount of tiles so that each tile is assigned the same amount of records"
                                              )
                                   .addOption("Smart Tile", "smart_tile",
                                              description="This option creates tiles based on the standard deviation of the values in the specified field. The tiles assigned indicate whether the record's value falls within the average range (=0), above the average (1), or below the average (-1)"
                                              )
                                   .addOption("Unique Value", "unique_value_tile",
                                              description="This option assign a unique tile for every unique value in a specified field or fields. If multiple fields are specified, a tile is assigned based on that combination of values"
                                              )
                                   .addOption("Manual", "manual_tile",
                                              description="The user can specify the cutoffs for the tiles by typing a value on a new line for each range"
                                              )
                                   .setOptionType("button")
                                   .setVariant("medium")
                                   .setButtonStyle("solid")
                                   .bindProperty("tile_method")
                                   )

        order_by_table = BasicTable(
            "OrderByTable",
            height="200px",
            columns=[
                Column(  # 1st column – expression editor
                    "Order By Columns",
                    "expression.expression",
                    ExpressionBox(ignoreTitle=True, language="sql")
                    .bindPlaceholders()
                    .withSchemaSuggestions()
                    .bindLanguage("${record.expression.format}"),
                    ),
                Column(  # 2nd column – ASC / DESC etc.
                    "Sort strategy",
                    "sortType",
                    SelectBox("")
                    .addOption("ascending nulls first", "asc")
                    .addOption("ascending nulls last", "asc_nulls_last")
                    .addOption("descending nulls first", "desc_nulls_first")
                    .addOption("descending nulls last", "desc"),
                    width="25%",
                    ),
            ],
        )

        equal_sum_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(TextBox("Number of Tiles").bindProperty("number_of_tiles").bindPlaceholder(""))
            .addElement(StackLayout(height="100%")
            .addElement(
                SchemaColumnsDropdown("Select Sum Column", appearance="default")
                .bindSchema("component.ports.inputs[0].schema")
                .bindProperty("sum_column_name")
            )
            )

            .addElement(SchemaColumnsDropdown("Select group by columns (Optional)", appearance="default")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("groupby_column_names"))
        )

        equal_records_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(TextBox("Number of Tiles").bindProperty("number_of_tiles").bindPlaceholder(""))
            .addElement(SchemaColumnsDropdown("Select group by columns (Optional)", appearance="default")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("groupby_column_names"))
            .addElement(SchemaColumnsDropdown("Do not split tile on Columns (Optional)", appearance="default")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("donot_split_tile_column_names"))
        )

        smart_tile_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(SchemaColumnsDropdown("Select Tile Numeric Column", appearance="default")
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("smart_tile_column_name")
                        )
            .addElement(SchemaColumnsDropdown("Select group by columns (Optional)", appearance="default")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("groupby_column_names"))
            .addElement(RadioGroup("")
                        .addOption("Do not output name column", "no_output_column_smartTile",
                                   description=("This option assigns tiles to cover a range of values where each tile has the same total of the Sum field based on the sort order of the incoming records"))
                        .addOption("Output name column", "output_column_smartTile",
                                   description=("An additional descriptive output field name is appended to the output. Descriptors include Average, Above Average, High, Extremely High, Below Average, Low, and Extremely Low"))
                        .addOption("Output verbose name column", "output_verbose_column_smartTile",
                                   description=("In addition to the descriptors mentioned above, the value range that the tile indicates is listed in parenthesis. For example, High (12750 to 155000)"))
                        .setOptionType("button")
                        .setVariant("medium")
                        .setButtonStyle("solid")
                        .bindProperty("column_output_method_smartTile")
                        )
        )

        unique_value_tile_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(SchemaColumnsDropdown("Select Unique Column", appearance="default")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("unique_value_column_name"))
            .addElement(SchemaColumnsDropdown("Select group by columns (Optional)", appearance="default")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("groupby_column_names"))
        )

        manual_tile_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(SchemaColumnsDropdown("Select Tile Numeric Column", appearance="default")
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("manual_tile_column_name")
                        )
            .addElement(TextBox("Enter one or more tile cutoffs").bindProperty("manual_tiles_cutoff").bindPlaceholder("Enter each tile's upper limit separated by comma in the box provided"))
        )

        return Dialog("tiling_dialog_box").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer()
                    .addElement(
                        Step()
                        .addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Select tiling method"))
                            .addElement(
                                select_tiling_radio_box
                            )
                        )
                    )
                )
                .addElement(
                    StepContainer()
                    .addElement(
                        Step()
                        .addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Select tiling options"))
                            .addElement(
                                Condition().ifEqual(PropExpr("component.properties.tile_method"), StringExpr("equal_sum_tile")).then(equal_sum_params_ui)
                            )
                            .addElement(
                                Condition().ifEqual(PropExpr("component.properties.tile_method"), StringExpr("equal_records_tile")).then(equal_records_params_ui)
                            )
                            .addElement(
                                Condition().ifEqual(PropExpr("component.properties.tile_method"), StringExpr("smart_tile")).then(smart_tile_params_ui)
                            )
                            .addElement(
                                Condition().ifEqual(PropExpr("component.properties.tile_method"), StringExpr("unique_value_tile")).then(unique_value_tile_params_ui)
                            )
                            .addElement(
                                Condition().ifEqual(PropExpr("component.properties.tile_method"), StringExpr("manual_tile")).then(manual_tile_params_ui)
                            )
                        )
                    )
                )
                .addElement(
                    Condition().ifEqual(PropExpr("component.properties.tile_method"), StringExpr("equal_sum_tile")).then(
                        StepContainer().addElement(
                            Step().addElement(
                                StackLayout(height="100%")
                                .addElement(TitleElement("Order rows within each group (Optional)"))
                                .addElement(order_by_table.bindProperty("orderByColumns"))
                            )
                        )
                    )
                )
                .addElement(
                    Condition().ifEqual(PropExpr("component.properties.tile_method"), StringExpr("equal_records_tile")).then(
                        StepContainer().addElement(
                            Step().addElement(
                                StackLayout(height="100%")
                                .addElement(TitleElement("Order rows within each group (Optional)"))
                                .addElement(order_by_table.bindProperty("orderByColumns"))
                            )
                        )
                    )
                )

            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(Tile, self).validate(context, component)
        schema_js = json.loads(component.properties.schema)
        colTypeMap = defaultdict(lambda:"")
        for col in schema_js:
            colTypeMap[col["name"]] = col["dataType"]

        if component.properties.tile_method in ("equal_sum_tile", "equal_records_tile"):
            num_tiles = component.properties.number_of_tiles
            if not num_tiles.isdigit():
                diagnostics.append(
                    Diagnostic("component.properties.number_of_tiles", f"Number of tiles should be an integer value greater than 0", SeverityLevelEnum.Error)
                )
            for idx, rule in enumerate(component.properties.orderByColumns):
                expr_text = (rule.expression.expression or "").strip()
                if rule.sortType and expr_text == "":
                    diagnostics.append(
                        Diagnostic(
                            f"component.properties.orderByColumns[{idx}].expression.expression",
                            "Order column expression is required when a sort direction is selected.",
                            SeverityLevelEnum.Error,
                        )
                    )
        if component.properties.tile_method == "equal_sum_tile":
            if component.properties.sum_column_name == "":
                diagnostics.append(
                    Diagnostic("component.properties.sum_column_name", f"Select one column for Sum Tile which should be of numeric type", SeverityLevelEnum.Error)
                )
            elif component.properties.sum_column_name not in component.properties.schema:
                diagnostics.append(
                    Diagnostic("component.properties.sum_column_name", f"Selected column {component.properties.sum_column_name} are not present in input schema.", SeverityLevelEnum.Error)
                )
            if colTypeMap[component.properties.sum_column_name] not in ("Bigint", "Integer", "Float", "Double", "Decimal"):
                diagnostics.append(
                    Diagnostic("component.properties.sum_column_name", f"Data Type for Sum Column should be of numeric type", SeverityLevelEnum.Error)
                )
        if component.properties.tile_method == "smart_tile":
            if component.properties.smart_tile_column_name == "":
                diagnostics.append(
                    Diagnostic("component.properties.smart_tile_column_name", f"Select one column for Smart Tile which should be of numeric type", SeverityLevelEnum.Error)
                )
            elif component.properties.smart_tile_column_name not in component.properties.schema:
                diagnostics.append(
                    Diagnostic("component.properties.smart_tile_column_name", f"Selected column {component.properties.smart_tile_column_name} are not present in input schema.", SeverityLevelEnum.Error)
                )
            if colTypeMap[component.properties.smart_tile_column_name] not in ("Bigint", "Integer", "Float", "Double"):
                diagnostics.append(
                    Diagnostic("component.properties.smart_tile_column_name", f"Data Type for Smart Tile Column should be of numeric type", SeverityLevelEnum.Error)
                )
        if component.properties.tile_method == "manual_tile":
            if component.properties.manual_tile_column_name == "":
                diagnostics.append(
                    Diagnostic("component.properties.manual_tile_column_name", f"Select one column for Manual Tile which should be of numeric type", SeverityLevelEnum.Error)
                )
            elif component.properties.manual_tile_column_name not in component.properties.schema:
                diagnostics.append(
                    Diagnostic("component.properties.manual_tile_column_name", f"Selected column {component.properties.manual_tile_column_name} are not present in input schema.", SeverityLevelEnum.Error)
                )
            if colTypeMap[component.properties.manual_tile_column_name] not in ("Bigint", "Integer", "Float", "Double"):
                diagnostics.append(
                    Diagnostic("component.properties.manual_tile_column_name", f"Data Type for Manual Tile Column should be of numeric type", SeverityLevelEnum.Error)
                )

            if component.properties.manual_tiles_cutoff == "":
                diagnostics.append(
                    Diagnostic("component.properties.manual_tiles_cutoff", f"Enter each tile's cutoff as upper limit comma separated in increasing order", SeverityLevelEnum.Error)
                )
            else:
                manual_tile_cutoff_list = (component.properties.manual_tiles_cutoff).split(',')
                trimmed = [s.strip() for s in manual_tile_cutoff_list]
                if not all(s.isdigit() for s in trimmed):
                    diagnostics.append(
                        Diagnostic("component.properties.manual_tiles_cutoff", f"Enter each tile's cutoff as upper limit comma separated in increasing order", SeverityLevelEnum.Error)
                    )
                else:
                    nums = list(map(int, trimmed))
                    is_sequential = all(nums[i] < nums[i+1] for i in range(len(nums) - 1))
                    if not is_sequential:
                        diagnostics.append(
                            Diagnostic("component.properties.manual_tiles_cutoff", f"Enter each tile's cutoff as upper limit comma separated in increasing order", SeverityLevelEnum.Error)
                        )
        if component.properties.tile_method == "unique_value_tile":
            if len(component.properties.unique_value_column_name) == 0:
                diagnostics.append(
                    Diagnostic("component.properties.unique_value_column_name", f"Select atleast column for Unique Tile", SeverityLevelEnum.Error)
                )
            elif len(component.properties.unique_value_column_name) > 0 :
                missingKeyColumns = [col for col in component.properties.unique_value_column_name if
                                     col not in component.properties.schema]
                if missingKeyColumns:
                    diagnostics.append(
                        Diagnostic("component.properties.unique_value_column_name", f"Selected columns {missingKeyColumns} are not present in input schema.", SeverityLevelEnum.Error)
                    )
        return diagnostics

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

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: TileProperties) -> str:
        # Generate the actual macro call given the component's state
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        resolved_macro_name = f"{self.projectName}.{self.name}"
        schema_columns = [js['name'] for js in json.loads(props.schema)]
        schema_columns_str = ", ".join(schema_columns)

        manual_tile_cutoff_list = (props.manual_tiles_cutoff).split(',')

        order_rules: List[dict] = [
            {"expr": expr, "sort": r.sortType}
            for r in props.orderByColumns
            for expr in [(r.expression.expression or "").strip()]  # temp var
            if expr  # keep non-empty
        ]

        def safe_str(val):
            if val is None or val == "":
                return "''"
            if isinstance(val, list):
                return str(val)
            return f"'{val}'"

        arguments = [
            safe_str(table_name),
            safe_str(props.tile_method),
            safe_str(props.number_of_tiles),
            safe_str(props.sum_column_name),
            safe_str(order_rules),
            safe_str(props.groupby_column_names),
            safe_str(props.smart_tile_column_name),
            safe_str(props.column_output_method_smartTile),
            safe_str(props.unique_value_column_name),
            safe_str(props.manual_tile_column_name),
            safe_str(manual_tile_cutoff_list),
            safe_str(props.donot_split_tile_column_names),
            safe_str(schema_columns_str)
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        raw_rel = parametersMap.get("relation_name") or ""
        relation_name = (
            json.loads(raw_rel.replace("'", '"'))
            if raw_rel.strip().startswith("[")
            else [s.strip() for s in raw_rel.lstrip("'").rstrip("'").split(",") if s.strip()]
        )
        return Tile.TileProperties(
            relation_name=relation_name,
            schema=parametersMap.get("schema"),
            orderByColumns=json.loads(
                parametersMap.get("orderByColumns").replace("'", '"')
            ),
            groupby_column_names=json.loads(
                parametersMap.get("groupby_column_names").replace("'", '"')
            ),
            unique_value_column_name=json.loads(
                parametersMap.get("unique_value_column_name").replace("'", '"')
            ),
            tile_method=parametersMap.get("tile_method").lstrip("'").rstrip("'"),
            number_of_tiles=parametersMap.get("number_of_tiles").lstrip("'").rstrip("'"),
            sum_column_name=parametersMap.get("sum_column_name").lstrip("'").rstrip("'"),
            smart_tile_column_name=parametersMap.get("smart_tile_column_name").lstrip("'").rstrip("'"),
            column_output_method_smartTile=parametersMap.get(
                "column_output_method_smartTile"
            ).lstrip("'").rstrip("'"),
            manual_tile_column_name=parametersMap.get(
                "manual_tile_column_name"
            ).lstrip("'").rstrip("'"),
            manual_tiles_cutoff=parametersMap.get("manual_tiles_cutoff").lstrip("'").rstrip("'"),
            donot_split_tile_column_names=json.loads(
                parametersMap.get("donot_split_tile_column_names").replace("'", '"')
            ),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("orderByColumns", json.dumps(properties.orderByColumns)),
                MacroParameter("groupby_column_names", json.dumps(properties.groupby_column_names)),
                MacroParameter("unique_value_column_name", json.dumps(properties.unique_value_column_name)),
                MacroParameter("tile_method", str(properties.tile_method)),
                MacroParameter("number_of_tiles", str(properties.number_of_tiles)),
                MacroParameter("sum_column_name", str(properties.sum_column_name)),
                MacroParameter("smart_tile_column_name", str(properties.smart_tile_column_name)),
                MacroParameter("column_output_method_smartTile", str(properties.column_output_method_smartTile)),
                MacroParameter("manual_tile_column_name", str(properties.manual_tile_column_name)),
                MacroParameter("manual_tiles_cutoff", str(properties.manual_tiles_cutoff)),
                MacroParameter(
                    "donot_split_tile_column_names",
                    json.dumps(properties.donot_split_tile_column_names),
                ),
            ],
        )
    def updateInputPortSlug(self, component: Component, context: SqlContext):
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)

