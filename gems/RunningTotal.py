import dataclasses
import json
from dataclasses import dataclass, field

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class ColumnExpr:
    expression: str
    format: Optional[str]


@dataclass(frozen=True)
class OrderByRule:
    expression: ColumnExpr
    sortType: str = "asc"


class RunningTotal(MacroSpec):
    name: str = "RunningTotal"
    projectName: str = "prophecy_basics"
    category: str = "Transform"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake, -> Need to get Expression Table expressions' quoting sorted.
        ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged,
    ]
    dependsOnUpstreamSchema: bool = True

    @dataclass(frozen=True)
    class RunningTotalProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        groupByColumnNames: List[str] = field(default_factory=list)
        runningTotalColumnNames: List[str] = field(default_factory=list)
        orderByColumns: List[OrderByRule] = field(default_factory=list)
        outputPrefix: Optional[str] = None

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

        return Dialog("RunningTotal").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Columns for running total"))
                            .addElement(
                                SchemaColumnsDropdown("", appearance="minimal")
                                .withMultipleSelection()
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("runningTotalColumnNames")
                            )
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Partition by (optional)"))
                            .addElement(
                                SchemaColumnsDropdown("", appearance="minimal")
                                .withMultipleSelection()
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("groupByColumnNames")
                            )
                            .addElement(
                                TitleElement("Order rows for calculation (optional)")
                            )
                            .addElement(order_by_table.bindProperty("orderByColumns"))
                            .addElement(
                                Condition()
                                .ifEqual(PropExpr("$.sql.metainfo.providerType"), StringExpr("bigquery"))
                                .then(
                                    AlertBox(
                                        variant="warning",
                                        _children=[
                                            Markdown(
                                                "**BigQuery:** BigQuery has no stable row identifier. Without **Order rows for calculation**, running total results will be non-deterministic."
                                            )
                                        ],
                                    )
                                )
                            )
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%").addElement(
                                TextBox("Output column prefix (optional)")
                                .bindPlaceholder("running_total_")
                                .bindProperty("outputPrefix")
                            )
                        )
                    )
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(RunningTotal, self).validate(context, component)

        if len(component.properties.runningTotalColumnNames) == 0:
            diagnostics.append(
                Diagnostic(
                    "properties.runningTotalColumnNames",
                    "Select at least one column to create running total.",
                    SeverityLevelEnum.Error,
                )
            )

        schema_js = json.loads(component.properties.schema or "[]")
        schema_cols_lower = set(f["name"].lower() for f in schema_js)

        if len(component.properties.groupByColumnNames) > 0:
            missingKeyColumns = [
                c
                for c in component.properties.groupByColumnNames
                if c.lower() not in schema_cols_lower
            ]
            if missingKeyColumns:
                diagnostics.append(
                    Diagnostic(
                        "properties.groupByColumnNames",
                        f"Selected columns {missingKeyColumns} are not present in input schema.",
                        SeverityLevelEnum.Error,
                    )
                )

        if len(component.properties.runningTotalColumnNames) > 0:
            missingRunningTotalColumns = [
                c
                for c in component.properties.runningTotalColumnNames
                if c.lower() not in schema_cols_lower
            ]
            if missingRunningTotalColumns:
                diagnostics.append(
                    Diagnostic(
                        "properties.runningTotalColumnNames",
                        f"Selected columns {missingRunningTotalColumns} are not present in input schema.",
                        SeverityLevelEnum.Error,
                    )
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

    def onChange(
            self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
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

    def apply(self, props: RunningTotalProperties) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"

        order_rules: List[dict] = [
            {
                "expression": {"expression": e},
                "sortType": r.sortType,
            }
            for r in props.orderByColumns
            for e in [(r.expression.expression or "").strip()]
            if e
        ]

        def safe_str(val):
            if val is None or val == "":
                return "''"
            if isinstance(val, list):
                return str(val)
            return f"'{val}'"

        prefix = (props.outputPrefix or "").strip() or "RunTot_"
        arguments = [
            str(props.relation_name),
            safe_str(props.groupByColumnNames),
            safe_str(props.runningTotalColumnNames),
            safe_str(prefix),
            str(order_rules),
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)

        def _parse_order_columns(raw: str) -> List[OrderByRule]:
            order_list = json.loads((raw or "[]").replace("'", '"'))
            return [
                OrderByRule(
                    expression=ColumnExpr(
                        expression=r.get("expression", {}).get("expression", "") or "",
                        format=r.get("expression", {}).get("format", "sql") or "sql",
                    ),
                    sortType=r.get("sortType", "asc") or "asc",
                )
                for r in order_list
            ]

        raw_rel = parametersMap.get("relation_name") or "[]"
        raw_group = parametersMap.get("groupByColumnNames") or "[]"
        raw_running = parametersMap.get("runningTotalColumnNames") or "[]"
        raw_order = parametersMap.get("orderByColumns") or "[]"

        output_prefix_raw = (parametersMap.get("outputPrefix") or "''").lstrip("'").rstrip("'")
        output_prefix = output_prefix_raw if output_prefix_raw and output_prefix_raw != "None" else "RunTot_"

        return RunningTotal.RunningTotalProperties(
            relation_name=json.loads(raw_rel.replace("'", '"')),
            schema=parametersMap.get("schema") or "",
            groupByColumnNames=json.loads(raw_group.replace("'", '"')),
            runningTotalColumnNames=json.loads(raw_running.replace("'", '"')),
            orderByColumns=_parse_order_columns(raw_order),
            outputPrefix=output_prefix,
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        order_by_json = json.dumps(
            [
                {
                    "expression": {
                        "expression": r.expression.expression or "",
                        "format": r.expression.format or "sql",
                    },
                    "sortType": r.sortType or "asc",
                }
                for r in (properties.orderByColumns or [])
            ]
        )
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", str(properties.schema or "")),
                MacroParameter(
                    "groupByColumnNames", json.dumps(properties.groupByColumnNames or [])
                ),
                MacroParameter(
                    "runningTotalColumnNames",
                    json.dumps(properties.runningTotalColumnNames or []),
                ),
                MacroParameter("orderByColumns", order_by_json),
                MacroParameter(
                    "outputPrefix",
                    str(properties.outputPrefix or "RunTot_"),
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
        from pyspark.sql.functions import sum as spark_sum, col, lit, coalesce, expr, monotonically_increasing_id 
        group_cols = self.props.groupByColumnNames
        running_total_cols = self.props.runningTotalColumnNames
        order_rules = self.props.orderByColumns
        
        order_cols = []
        for r in order_rules:
            if (r.expression.expression):
                e = expr(r.expression.expression.strip())
                if r.sortType == "asc":
                    order_cols.append(e.asc())
                elif r.sortType == "asc_nulls_last":
                    order_cols.append(e.asc_nulls_last())
                elif r.sortType == "desc_nulls_first":
                    order_cols.append(e.desc_nulls_first())
                else:
                    order_cols.append(e.desc())

        if not order_cols:
            order_cols = [monotonically_increasing_id().asc()]

        if group_cols:
            col_group_cols = [col(c) for c in group_cols]
            window_spec = (
                Window.partitionBy(*col_group_cols)
                .orderBy(*order_cols)
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
        else:
            window_spec = Window.orderBy(*order_cols).rowsBetween(
                Window.unboundedPreceding, Window.currentRow
            )

        result = in0
        for col_name in running_total_cols:
            run_tot_col = str(self.props.outputPrefix) + col_name
            result = result.withColumn(
                run_tot_col,
                spark_sum(coalesce(col(col_name), lit(0))).over(window_spec),
            )

        return result
