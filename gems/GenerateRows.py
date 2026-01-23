import dataclasses
import json

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class GenerateRows(MacroSpec):
    name: str = "GenerateRows"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake,
        # ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]
    dependsOnUpstreamSchema: bool = False

    @dataclass(frozen=True)
    class GenerateRowsProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        init_expr: Optional[str] = None
        condition_expr: Optional[str] = None
        loop_expr: Optional[str] = None
        column_name: Optional[str] = None
        max_rows: Optional[str] = None
        force_mode: Optional[str] = "recursive"

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
        row_generation = StepContainer().addElement(
            Step().addElement(
                StackLayout(height="100%")
                .addElement(
                    TitleElement("*Configure row generation strategy")
                )
                .addElement(TextBox("Initialization expression").bindPlaceholder(
                    """Row generation would start from this. eg: 1 or payload.col_name""").bindProperty("init_expr"))
                .addElement(TextBox("Condition expression").bindPlaceholder(
                    """Breaking condition to stop loop. eg: value < 10 or value < payload.col_name""").bindProperty(
                    "condition_expr"))
                .addElement(TextBox("Loop expression (usually incremental)").bindPlaceholder(
                    """Step to increment with in every iteration. eg: value + 1 or value + payload.col_name""").bindProperty(
                    "loop_expr"))
            )
        )

        create_column = StepContainer().addElement(
            Step().addElement(
                StackLayout(height="100%")
                .addElement(
                    TitleElement("*Choose output column name")
                )
                .addElement(TextBox("").bindPlaceholder(
                    """value""").bindProperty("column_name"))
            )
        )

        advanced_options = StepContainer().addElement(
            Step().addElement(
                StackLayout(height="100%")
                .addElement(
                    TitleElement("Advanced options")
                )
                .addElement(TextBox("Max rows per iteration (default: 100000)").bindPlaceholder(
                    """100000""").bindProperty("max_rows"))
            )
        )

        return Dialog("GenerateRows").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True, allowCustomOutputSchema=True),
                "content"
            )
            .addColumn(
                StackLayout(height="100%")
                .addElement(create_column)
                .addElement(row_generation)
                .addElement(advanced_options)
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super().validate(context, component)
        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: GenerateRowsProperties) -> str:

        # generate the actual macro call given the component's
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            str(props.relation_name),
            "'" + str(props.init_expr) + "'",
            "'" + str(props.condition_expr) + "'",
            "'" + str(props.loop_expr) + "'",
            "'" + str(props.column_name) + "'",
            "'" + str(props.max_rows) + "'",
            "'" + str(props.force_mode) + "'"
        ]

        params = ",".join(arguments)
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    # --- GenerateRows.loadProperties -----------------------------------------
    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        p = self.convertToParameterMap(properties.parameters)

        # --- turn the stored text "['seed1']" back into a real list -------------
        raw_rel = p.get('relation_name', '')
        try:
            relation_name_list = (
                json.loads(raw_rel.replace("'", '"'))
                if raw_rel.strip() not in ['', '[]']
                else []
            )
        except json.JSONDecodeError:
            # if it's something like 'seed1' just wrap it in a list
            relation_name_list = [raw_rel.strip()] if raw_rel.strip() else []

        return GenerateRows.GenerateRowsProperties(
            relation_name=relation_name_list,  # <-- now always a list
            init_expr=p.get('init_expr').lstrip("'").rstrip("'"),
            condition_expr=p.get('condition_expr').lstrip("'").rstrip("'"),
            loop_expr=p.get('loop_expr').lstrip("'").rstrip("'"),
            column_name=p.get('column_name').lstrip("'").rstrip("'"),
            max_rows=p.get('max_rows').lstrip("'").rstrip("'"),
            force_mode=p.get('force_mode').lstrip("'").rstrip("'"),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("init_expr", properties.init_expr),
                MacroParameter("condition_expr", properties.condition_expr),
                MacroParameter("loop_expr", properties.loop_expr),
                MacroParameter("column_name", properties.column_name),
                MacroParameter("max_rows", properties.max_rows),
                MacroParameter("force_mode", properties.force_mode)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)
