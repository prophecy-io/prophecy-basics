import dataclasses
import json
from dataclasses import dataclass, field
from typing import List

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


@dataclass(frozen=True)
class ColumnExpr:
    expression: str
    format: str


@dataclass(frozen=True)
class OrderByRule:
    expression: ColumnExpr
    sortType: str = "asc"


class RecordID(MacroSpec):
    name: str = "RecordID"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1

    @dataclass(frozen=True)
    class RecordIDProperties(MacroProperties):
        method: str = "incremental_id"
        incremental_id_column_name: str = "RecordID"
        incremental_id_starting_val: int = 1000
        incremental_id_type: str = "string"
        incremental_id_size: int = 6
        position: str = "first_column"
        generationMethod: str = "tableLevel"
        groupByColumnNames: List[str] = field(default_factory=list)
        relation_name: List[str] = field(default_factory=list)
        orders: List[OrderByRule] = field(default_factory=list)

    # -------------------------------------------------------------------------
    # Utility methods
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # UI definition
    # -------------------------------------------------------------------------
    def dialog(self) -> Dialog:
        order_by_table = BasicTable(
            "OrderByTable",
            height="200px",
            columns=[
                Column(
                    "Order By Columns",
                    "expression.expression",
                    ExpressionBox(ignoreTitle=True, language="sql")
                    .bindPlaceholders()
                    .withSchemaSuggestions()
                    .bindLanguage("${record.expression.format}"),
                ),
                Column(
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

        generationMethod = (
            Condition()
            .ifEqual(
                PropExpr("component.properties.method"),
                StringExpr("incremental_id"),
            )
            .then(
                StepContainer().addElement(
                    Step().addElement(
                        StackLayout(height="100%").addElement(
                            RadioGroup("Record ID Generation Scope")
                            .addOption(
                                "Across entire table",
                                "tableLevel",
                                description="Generate a unique record ID for each row across the full dataset (no grouping).",
                            )
                            .addOption(
                                "Within each group",
                                "groupLevel",
                                description="Generate a unique record ID within each group defined by selected column(s).",
                            )
                            .setOptionType("button")
                            .setVariant("medium")
                            .setButtonStyle("solid")
                            .bindProperty("generationMethod")
                        )
                    )
                )
            )
        )

        return Dialog("Macro").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(gap="1rem", height="100%")
                .addElement(
                    SelectBox("Record Id generation method")
                    .addOption("UUID", "uuid")
                    .addOption("Incremental ID", "incremental_id")
                    .bindProperty("method")
                )
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.method"),
                        StringExpr("uuid"),
                    )
                    .then(
                        TextBox(
                            "Output Column Name", placeholder="RecordID"
                        ).bindProperty("incremental_id_column_name")
                    )
                )
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.method"),
                        StringExpr("incremental_id"),
                    )
                    .then(
                        ColumnsLayout(gap="1rem")
                        .addColumn(
                            TextBox(
                                "Output Column Name", placeholder="RecordID"
                            ).bindProperty("incremental_id_column_name")
                        )
                        .addColumn(
                            NumberBox(
                                "Starting Value",
                                placeholder="1000",
                                minValueVar=1,
                            ).bindProperty("incremental_id_starting_val")
                        )
                    )
                )
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.method"),
                        StringExpr("incremental_id"),
                    )
                    .then(
                        StackLayout().addElement(
                            ColumnsLayout(gap="1rem")
                            .addColumn(
                                SelectBox("Data type")
                                .addOption("string", "string")
                                .addOption("integer", "integer")
                                .bindProperty("incremental_id_type")
                            )
                            .addColumn(
                                Condition()
                                .ifEqual(
                                    PropExpr(
                                        "component.properties.incremental_id_type"
                                    ),
                                    StringExpr("string"),
                                )
                                .then(
                                    NumberBox(
                                        "Left-pad length",
                                        placeholder="6",
                                        minValueVar=0,
                                        maxValueVar=100,
                                    ).bindProperty("incremental_id_size")
                                )
                            )
                        )
                    )
                )
                .addElement(
                    SelectBox("Column position")
                    .addOption("add as first column", "first_column")
                    .addOption("add as last column", "last_column")
                    .bindProperty("position")
                )
                .addElement(generationMethod)
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.generationMethod"),
                        StringExpr("groupLevel"),
                    )
                    .then(
                        StepContainer().addElement(
                            Step().addElement(
                                StackLayout(height="100%")
                                .addElement(
                                    TitleElement("Configure Grouping and Sorting")
                                )
                                .addElement(TitleElement("Group By Columns"))
                                .addElement(
                                    SchemaColumnsDropdown("")
                                    .withMultipleSelection()
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("groupByColumnNames")
                                )
                                .addElement(
                                    TitleElement(
                                        "Order rows within each group (Optional)"
                                    )
                                )
                                .addElement(order_by_table.bindProperty("orders"))
                            )
                        )
                    )
                )
            )
        )

    # -------------------------------------------------------------------------
    # Validation
    # -------------------------------------------------------------------------
    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(RecordID, self).validate(context, component)
        props = component.properties

        # 1. OrderBy grid
        for idx, rule in enumerate(props.orders):
            expr_text = (rule.expression.expression or "").strip()
            if rule.sortType and expr_text == "":
                diagnostics.append(
                    Diagnostic(
                        f"properties.orders[{idx}].expression.expression",
                        "Order column expression is required when a sort direction is selected.",
                        SeverityLevelEnum.Error,
                    )
                )

        # 2. Record-ID column name
        if not (props.incremental_id_column_name or "").strip():
            diagnostics.append(
                Diagnostic(
                    "properties.incremental_id_column_name",
                    "Column name for the generated Record ID must not be empty.",
                    SeverityLevelEnum.Error,
                )
            )

        # 3. Group-by column validation
        field_names_upper = {
            f["name"].upper() for f in component.ports.inputs[0].schema["fields"]
        }

        if props.groupByColumnNames:
            missing = [
                c
                for c in props.groupByColumnNames
                if c.upper() not in field_names_upper
            ]
            if missing:
                diagnostics.append(
                    Diagnostic(
                        "properties.groupByColumnNames",
                        f"Group-by column(s) not found in input schema: {', '.join(missing)}",
                        SeverityLevelEnum.Error,
                    )
                )

        return diagnostics

    # -------------------------------------------------------------------------
    # State change handler
    # -------------------------------------------------------------------------
    def onChange(
        self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
        relation_name = self.get_relation_names(newState, context)
        newProperties = dataclasses.replace(
            newState.properties, relation_name=relation_name
        )

        if (
            oldState.properties.method == "incremental_id"
            and newState.properties.method == "uuid"
        ):
            return newState.bindProperties(
                dataclasses.replace(newProperties, generationMethod="tableLevel")
            )

        return newState.bindProperties(newProperties)

    # -------------------------------------------------------------------------
    # Macro application
    # -------------------------------------------------------------------------
    def apply(self, props: RecordIDProperties) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        order_rules: List[dict] = [
            {"expr": expr, "sort": r.sortType}
            for r in props.orders
            for expr in [(r.expression.expression or "").strip()]
            if expr
        ]

        arguments = [
            f"'{table_name}'",
            f"'{props.method}'",
            f"'{props.incremental_id_column_name}'",
            f"'{props.incremental_id_type}'",
            str(props.incremental_id_size),
            str(props.incremental_id_starting_val),
            f"'{props.generationMethod}'",
            f"'{props.position}'",
            str(props.groupByColumnNames),
            str(order_rules),
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    # -------------------------------------------------------------------------
    # Property loading/unloading
    # -------------------------------------------------------------------------
    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        return RecordID.RecordIDProperties(
            relation_name=parametersMap.get("relation_name"),
            method=parametersMap.get("method"),
            incremental_id_column_name=parametersMap.get("incremental_id_column_name"),
            incremental_id_type=parametersMap.get("incremental_id_type"),
            incremental_id_size=float(parametersMap.get("incremental_id_size")),
            incremental_id_starting_val=float(
                parametersMap.get("incremental_id_starting_val")
            ),
            generationMethod=parametersMap.get("generationMethod"),
            position=parametersMap.get("position"),
            groupByColumnNames=json.loads(
                parametersMap.get("groupByColumnNames").replace("'", '"')
            ),
            orders=parametersMap.get("orders"),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("method", properties.method),
                MacroParameter(
                    "incremental_id_column_name", properties.incremental_id_column_name
                ),
                MacroParameter("incremental_id_type", properties.incremental_id_type),
                MacroParameter(
                    "incremental_id_size", str(properties.incremental_id_size)
                ),
                MacroParameter(
                    "incremental_id_starting_val",
                    str(properties.incremental_id_starting_val),
                ),
                MacroParameter("generationMethod", properties.generationMethod),
                MacroParameter("position", properties.position),
                MacroParameter(
                    "groupByColumnNames", json.dumps(properties.groupByColumnNames)
                ),
                MacroParameter("orders", str(properties.orders)),
            ],
        )

    # -------------------------------------------------------------------------
    # Port slug update
    # -------------------------------------------------------------------------
    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)
        newProperties = dataclasses.replace(
            component.properties, relation_name=relation_name
        )
        return component.bindProperties(newProperties)
