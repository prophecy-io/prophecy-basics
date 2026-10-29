import ast
import dataclasses
import json

import re
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class TestTrainSplit(MacroSpec):
    name: str = "TestTrainSplit"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1
    # out0 = train, out1 = test, out2 = validation
    defaultOutputPorts: list[str] = ["out0", "out1", "out2"]
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
    ]
    dependsOnUpstreamSchema: bool = False

    # Split returned on each output port, in port order
    splitNames: list[str] = ["train", "test", "validation"]

    @dataclass(frozen=True)
    class TestTrainSplitProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        # whole-number percentages; the property spec parser has no float kind
        train_pct: int = 70
        test_pct: int = 20
        validation_pct: int = 10
        seed: int = 42

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

    @staticmethod
    def _is_empty_schema(schema) -> bool:
        # A schema is None or "" until the upstream schema has been inferred
        return schema is None or (isinstance(schema, str) and not schema.strip())

    def dialog(self) -> Dialog:
        percentages = (
            ColumnsLayout(gap="1rem")
            .addColumn(NumberBox("Train %", placeholder=70).bindProperty("train_pct"))
            .addColumn(NumberBox("Test %", placeholder=20).bindProperty("test_pct"))
            .addColumn(NumberBox("Validation %", placeholder=10).bindProperty("validation_pct"))
        )

        dialog = Dialog("test_train_split_dialog_box").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(allowOutputRename=True), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Split percentages (whole numbers, must add up to 100)"))
                            .addElement(percentages)
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            NumberBox("Random seed", placeholder=42).bindProperty("seed")
                        )
                    )
                )
                .addElement(
                    AlertBox(
                        variant="success",
                        _children=[
                            Markdown(
                                "Output ports: **out0** = train, **out1** = test, **out2** = validation. \n\n"
                                "Rows are assigned by a seeded hash of the whole row, so the split is "
                                "deterministic and sizes follow the percentages approximately."
                            )
                        ],
                    )
                )
            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(TestTrainSplit, self).validate(context, component)
        props = component.properties
        percentages = {
            "train_pct": props.train_pct,
            "test_pct": props.test_pct,
            "validation_pct": props.validation_pct,
        }
        for prop_name, value in percentages.items():
            if value is None or not (0 <= int(value) <= 100):
                diagnostics.append(
                    Diagnostic(
                        f"component.properties.{prop_name}",
                        "Percentage must be between 0 and 100",
                        SeverityLevelEnum.Error,
                    )
                )
        if all(v is not None for v in percentages.values()) and sum(int(v) for v in percentages.values()) != 100:
            diagnostics.append(
                Diagnostic(
                    "component.properties.train_pct",
                    "Train, test and validation percentages must add up to 100",
                    SeverityLevelEnum.Error,
                )
            )
        if len(component.ports.outputs) != len(self.splitNames):
            diagnostics.append(
                Diagnostic(
                    "component.ports.outputs",
                    f"TestTrainSplit needs exactly {len(self.splitNames)} output ports (train, test, validation)",
                    SeverityLevelEnum.Error,
                )
            )
        return diagnostics

    def onChange(
        self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
        # Handle changes in the component's state and return the new state
        if self._is_empty_schema(newState.ports.inputs[0].schema):
            raise ValueError(f"{self.name}: input schema is empty")
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
        return self._with_port_hints(newState.bindProperties(newProperties))

    def _with_port_hints(self, component: Component) -> Component:
        # Tell the UI what each output port holds: its share of the data, its icon and the input it comes from
        props = component.properties
        percentages = [props.train_pct, props.test_pct, props.validation_pct]
        outputs = list(component.ports.outputs)
        for idx, (split_name, pct) in enumerate(zip(self.splitNames, percentages)):
            if idx >= len(outputs):
                break
            outputs[idx] = dataclasses.replace(
                outputs[idx],
                description=f"Contains {pct}% of the data ({split_name})",
                icon=PortIconEnum.PercentB,
                incomingAliases=list(props.relation_name),
            )
        return component.bindPorts(dataclasses.replace(component.ports, outputs=outputs))

    def _macro_call(self, props: TestTrainSplitProperties, split_name: str) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            str(props.relation_name),
            f"'{split_name}'",
            str(props.train_pct),
            str(props.test_pct),
            str(props.validation_pct),
            str(props.seed),
        ]
        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def apply(self, props: TestTrainSplitProperties) -> str:
        # Single-port fallback: only the train split
        return self._macro_call(props, self.splitNames[0])

    def applyMulti(self, props: TestTrainSplitProperties, outputPorts: list[str]) -> list[str]:
        # One macro call per output port: out0 = train, out1 = test, out2 = validation
        return [self._macro_call(props, split_name) for split_name in self.splitNames[: len(outputPorts)]]

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)

        def _parse_py_literal(raw, default):
            # apply() emits list params as str(<python value>) (single-quoted
            # repr), so the inverse is ast.literal_eval — NOT json.loads
            raw = (raw or "").strip()
            if not raw:
                return default
            try:
                return ast.literal_eval(raw)
            except (ValueError, SyntaxError):
                return default

        return TestTrainSplit.TestTrainSplitProperties(
            relation_name=_parse_py_literal(parametersMap.get('relation_name'), []),
            schema=parametersMap.get("schema"),
            train_pct=_parse_py_literal(parametersMap.get("train_pct"), 70),
            test_pct=_parse_py_literal(parametersMap.get("test_pct"), 20),
            validation_pct=_parse_py_literal(parametersMap.get("validation_pct"), 10),
            seed=_parse_py_literal(parametersMap.get("seed"), 42),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("train_pct", str(properties.train_pct)),
                MacroParameter("test_pct", str(properties.test_pct)),
                MacroParameter("validation_pct", str(properties.validation_pct)),
                MacroParameter("seed", str(properties.seed)),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        if self._is_empty_schema(component.ports.inputs[0].schema):
            raise ValueError(f"{self.name}: input schema is empty")
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
        return self._with_port_hints(component.bindProperties(newProperties))
