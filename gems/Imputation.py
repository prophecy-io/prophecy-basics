import dataclasses
import json

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


class Imputation(MacroSpec):
    name: str = "Imputation"
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
    class ImputationProperties(MacroProperties):
        schema: str = ""
        relation_name: List[str] = field(default_factory=list)
        columnNames: List[str] = field(default_factory=list)
        replaceIncomingType: str = "null"
        incomingUserValue: str = ""
        replaceWithType: str = "average"
        replaceWithUserValue: str = ""
        includeImputedIndicator: bool = False
        outputImputedAsSeparateField: bool = False

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
        fieldsToImpute = (
            SchemaColumnsDropdown("Fields to impute", appearance="minimal")
            .withMultipleSelection()
            .bindSchema("component.ports.inputs[0].schema")
            .bindProperty("columnNames")
        )

        incomingSelect = (
            SelectBox("Incoming value to replace")
            .addOption("Null()", "null")
            .addOption("User specified value", "user")
            .bindProperty("replaceIncomingType")
        )

        incomingTextBox = (
            TextBox("Value to replace", placeholder="e.g. 0")
            .bindProperty("incomingUserValue")
        )

        replaceWithSelect = (
            SelectBox("Replace with value")
            .addOption("Average", "average")
            .addOption("Median", "median")
            .addOption("Mode", "mode")
            .addOption("User specified value", "user")
            .bindProperty("replaceWithType")
        )

        replaceWithTextBox = (
            TextBox("Replacement value", placeholder="e.g. 0")
            .bindProperty("replaceWithUserValue")
        )

        return Dialog("Imputation").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(fieldsToImpute)
                .addElement(incomingSelect)
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.replaceIncomingType"),
                        StringExpr("user"),
                    )
                    .then(incomingTextBox)
                )
                .addElement(replaceWithSelect)
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.replaceWithType"),
                        StringExpr("user"),
                    )
                    .then(replaceWithTextBox)
                )
                .addElement(
                    Checkbox("Include imputed value indicator field")
                    .bindProperty("includeImputedIndicator")
                )
                .addElement(
                    Checkbox("Output imputed values as a separate field")
                    .bindProperty("outputImputedAsSeparateField")
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(Imputation, self).validate(context, component)
        props = component.properties

        if props.replaceIncomingType == "user" and (props.incomingUserValue is None or str(props.incomingUserValue).strip() == ""):
            diagnostics.append(
                Diagnostic(
                    "component.properties.incomingUserValue",
                    "Please enter a value to replace when using User specified value.",
                    SeverityLevelEnum.Error,
                )
            )

        if props.replaceWithType == "user" and (props.replaceWithUserValue is None or str(props.replaceWithUserValue).strip() == ""):
            diagnostics.append(
                Diagnostic(
                    "component.properties.replaceWithUserValue",
                    "Please enter a replacement value when using User specified value.",
                    SeverityLevelEnum.Error,
                )
            )

        try:
            schema_data = json.loads(props.schema) if props.schema else []
        except Exception:
            schema_data = []

        fields_list = schema_data if isinstance(schema_data, list) else (schema_data.get("fields", []) if isinstance(schema_data, dict) else [])
        if len(component.properties.columnNames) > 0 and fields_list:
            schema_cols_lower = set(col.get("name", "").lower() for col in fields_list)
            missing = [c for c in component.properties.columnNames if c.lower() not in schema_cols_lower]
            if missing:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.columnNames",
                        f"Selected columns {missing} are not present in input schema.",
                        SeverityLevelEnum.Error,
                    )
                )

        return diagnostics

    def onChange(
        self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
        try:
            raw = str(newState.ports.inputs[0].schema).replace("'", '"')
            schema = json.loads(raw)
        except Exception:
            schema = {"fields": []}
        fields_array = [
            {"name": f.get("name"), "dataType": f.get("dataType", {}).get("type", "string")}
            for f in schema.get("fields", [])
        ]
        relation_name = self.get_relation_names(newState, context)
        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: ImputationProperties) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            str(props.relation_name),
            props.schema,
            str(props.columnNames),
            "'" + props.replaceIncomingType + "'",
            "'" + str(props.incomingUserValue).replace("'", "''") + "'",
            "'" + props.replaceWithType + "'",
            "'" + str(props.replaceWithUserValue).replace("'", "''") + "'",
            str(props.includeImputedIndicator).lower(),
            str(props.outputImputedAsSeparateField).lower(),
        ]
        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        return Imputation.ImputationProperties(
            relation_name=json.loads(parametersMap.get("relation_name", "[]").replace("'", '"')),
            schema=parametersMap.get("schema", "[]"),
            columnNames=json.loads(parametersMap.get("columnNames", "[]").replace("'", '"')),
            replaceIncomingType=parametersMap.get("replaceIncomingType", "null").strip("'"),
            incomingUserValue=parametersMap.get("incomingUserValue", "").strip("'").replace("''", "'"),
            replaceWithType=parametersMap.get("replaceWithType", "average").strip("'"),
            replaceWithUserValue=parametersMap.get("replaceWithUserValue", "").strip("'").replace("''", "'"),
            includeImputedIndicator=parametersMap.get("includeImputedIndicator", "false").lower() == "true",
            outputImputedAsSeparateField=parametersMap.get("outputImputedAsSeparateField", "false").lower() == "true",
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("columnNames", json.dumps(properties.columnNames)),
                MacroParameter("replaceIncomingType", "'" + properties.replaceIncomingType + "'"),
                MacroParameter("incomingUserValue", "'" + str(properties.incomingUserValue).replace("'", "''") + "'"),
                MacroParameter("replaceWithType", "'" + properties.replaceWithType + "'"),
                MacroParameter("replaceWithUserValue", "'" + str(properties.replaceWithUserValue).replace("'", "''") + "'"),
                MacroParameter("includeImputedIndicator", str(properties.includeImputedIndicator).lower()),
                MacroParameter("outputImputedAsSeparateField", str(properties.outputImputedAsSeparateField).lower()),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        try:
            raw = str(component.ports.inputs[0].schema).replace("'", '"')
            schema = json.loads(raw)
        except Exception:
            schema = {"fields": []}
        fields_array = [
            {"name": f.get("name"), "dataType": f.get("dataType", {}).get("type", "string")}
            for f in schema.get("fields", [])
        ]
        relation_name = self.get_relation_names(component, context)
        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
        )
        return component.bindProperties(newProperties)
