import ast
import dataclasses
import json

import re
from jinja2 import Environment, TemplateSyntaxError, nodes
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import *
from pyspark.sql.functions import *


class ToDo(MacroSpec):
    name: str = "ToDo"
    projectName: str = "prophecy_basics"
    category: str = "Custom"
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]
    dependsOnUpstreamSchema: bool = False

    @dataclass(frozen=True)
    class ToDoProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        error_string: Optional[str] = None
        code_string: Optional[str] = None
        diag_message: Optional[str] = None

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
        return Dialog("ToDo").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True, allowCustomOutputSchema=True),
                "content",
            )
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Highlight message"))
                            .addElement(
                                TextBox("")
                                .bindPlaceholder(
                                    "To-Do logic not implemented yet. Please complete this feature."
                                )
                                .bindProperty("diag_message")
                            )
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout()
                            .addElement(TitleElement("Error message (Optional)"))
                            .addElement(
                                TextBox("")
                                .bindPlaceholder(
                                    "Please enter error message here for reference."
                                )
                                .bindProperty("error_string")
                            )
                        )
                    )
                )
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout()
                            .addElement(TitleElement("Helper code/text (Optional)"))
                            .addElement(
                                TextArea("", 12)
                                .bindPlaceholder(
                                    "Paste sample code or helpful notes here for reference."
                                )
                                .bindProperty("code_string")
                            )
                        )
                    )
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super().validate(context, component)
        if (
            component.properties.diag_message is not None
            and component.properties.diag_message != ""
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.diag_message",
                    component.properties.diag_message,
                    SeverityLevelEnum.Error,
                )
            )
        else:
            diagnostics.append(
                Diagnostic(
                    "component.properties.diag_message",
                    "Highlight message field cannot be empty.",
                    SeverityLevelEnum.Error,
                )
            )
        return diagnostics

    def onChange(
        self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties, relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    @staticmethod
    def _jinja_constant(source: Optional[str]) -> Any:
        try:
            parsed = Environment().parse("{{ " + (source or "") + " }}")
        except (TemplateSyntaxError, TypeError):
            return None
        if (len(parsed.body) != 1 or not isinstance(parsed.body[0], nodes.Output)
                or len(parsed.body[0].nodes) != 1):
            return None
        node = parsed.body[0].nodes[0]
        if isinstance(node, nodes.Const) and isinstance(node.value, str):
            return node.value
        if isinstance(node, nodes.List) and all(
                isinstance(i, nodes.Const) and isinstance(i.value, str) for i in node.items):
            return [i.value for i in node.items]
        return None

    # Only unloadProperties writes these; the macro call never carries them.
    _SAVED_ONLY_PARAMETERS = ("error_string", "code_string")

    @staticmethod
    def _text(raw: Optional[str], from_code: bool) -> Optional[str]:
        if raw is None or not from_code:
            return raw
        value = ToDo._jinja_constant(raw)
        if isinstance(value, str):
            return value
        # ToDo(''msg''), written by older versions of this gem
        while len(raw) >= 4 and raw.startswith("''") and raw.endswith("''"):
            raw = raw[2:-2]
        return raw

    @staticmethod
    def _relations(raw: Optional[str]) -> List[str]:
        value = ToDo._jinja_constant(raw)
        if value is None and raw:
            try:
                value = ast.literal_eval(raw)
            except (ValueError, SyntaxError):
                value = None
        if isinstance(value, str):
            value = [value]
        return [str(v) for v in value] if isinstance(value, (list, tuple)) else []

    def apply(self, props: ToDoProperties) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"
        diagMessage: str = (
            props.diag_message
            if props.diag_message is not None
            else "No diaganostic provided."
        )
        # relation_name lets the SQL Editor reconnect the gem's inputs when it reads the code back
        arguments = [
            json.dumps(diagMessage, ensure_ascii=False),
            str([str(r) for r in (props.relation_name or []) if r]),
        ]
        params = ", ".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # Load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        from_code = not any(p in parametersMap for p in ToDo._SAVED_ONLY_PARAMETERS)
        return ToDo.ToDoProperties(
            relation_name=ToDo._relations(parametersMap.get("relation_name")),
            error_string=ToDo._text(parametersMap.get("error_string"), from_code) or None,
            code_string=ToDo._text(parametersMap.get("code_string"), from_code) or None,
            diag_message=ToDo._text(parametersMap.get("diag_message"), from_code),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("diag_message", properties.diag_message),
                MacroParameter("relation_name", json.dumps(properties.relation_name or [])),
                MacroParameter("error_string", properties.error_string or ""),
                MacroParameter("code_string", properties.code_string or ""),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties, relation_name=relation_name
        )
        return component.bindProperties(newProperties)

    def applyPython(self, spark: SparkSession, *inDFs: DataFrame) -> DataFrame:
        message = self.props.diag_message
        if message is not None:
            raise Exception(f"ToDo: {message}")
        return spark.sql("select 1")
