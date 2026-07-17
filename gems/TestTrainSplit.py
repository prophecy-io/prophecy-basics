import dataclasses
import json
import re

from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class TestTrainSplit(MacroSpec):
    name: str = "TestTrainSplit"
    projectName: str = "prophecy_basics"
    category: str = "Join/Split"
    minNumOfInputPorts: int = 1
    minNumOfOutputPorts: int = 2
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]
    dependsOnUpstreamSchema: bool = False

    @dataclass(frozen=True)
    class TestTrainSplitProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        split_column: str = ""
        train_percentage: int = 80

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
        return Dialog("TestTrainSplit").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(TitleElement("Split settings"))
                .addElement(
                    SchemaColumnsDropdown("Column to split on", appearance="minimal")
                    .bindSchema("component.ports.inputs[0].schema")
                    .bindProperty("split_column")
                )
                .addElement(
                    NumberBox("Train percentage (0-100)", placeholder="80")
                    .bindProperty("train_percentage")
                ),
                "5fr",
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(TestTrainSplit, self).validate(context, component)
        if component.properties.split_column == "":
            diagnostics.append(Diagnostic(
                "component.properties.split_column",
                "Select a column to split on",
                SeverityLevelEnum.Error,
            ))
        if not (0 < component.properties.train_percentage < 100):
            diagnostics.append(Diagnostic(
                "component.properties.train_percentage",
                "Train percentage must be strictly between 0 and 100",
                SeverityLevelEnum.Error,
            ))
        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        newProperties = dataclasses.replace(
            newState.properties,
            relation_name=self.get_relation_names(newState, context),
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: TestTrainSplitProperties) -> str:
        # Schema-analysis representative query (see NodeBuildActor.processWithOutputSchema):
        # train and test are a row-level split of the same input, so they always share the
        # same columns - the "train" branch alone is a correct stand-in for both output ports.
        resolved_macro_name = f"{self.projectName}.{self.name}"
        relation_arg = str(props.relation_name)
        return f"{{{{ {resolved_macro_name}({relation_arg}, '{props.split_column}', {props.train_percentage}, 'train') }}}}"

    def applyMulti(self, props: TestTrainSplitProperties, outputPorts: List[NodePort]) -> List[str]:
        # out0 = train, out1 = test. Both branches hash the SAME split_column with the SAME
        # deterministic function and compare against the SAME threshold (see
        # macros/TestTrainSplit.sql) - even though they compile to two fully independent
        # queries with no shared state, the complementary "< threshold" / ">= threshold"
        # conditions guarantee no row lands in both and every row lands in exactly one.
        resolved_macro_name = f"{self.projectName}.{self.name}"
        relation_arg = str(props.relation_name)
        return [
            f"{{{{ {resolved_macro_name}({relation_arg}, '{props.split_column}', {props.train_percentage}, 'train') }}}}",
            f"{{{{ {resolved_macro_name}({relation_arg}, '{props.split_column}', {props.train_percentage}, 'test') }}}}",
        ]

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        return TestTrainSplit.TestTrainSplitProperties(
            relation_name=json.loads(parametersMap.get("relation_name", "[]").replace("'", '"')),
            split_column=parametersMap.get("split_column", "").strip("'"),
            train_percentage=int(parametersMap.get("train_percentage", "80")),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("split_column", properties.split_column),
                MacroParameter("train_percentage", str(properties.train_percentage)),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        newProperties = dataclasses.replace(
            component.properties,
            relation_name=self.get_relation_names(component, context),
        )
        return component.bindProperties(newProperties)
