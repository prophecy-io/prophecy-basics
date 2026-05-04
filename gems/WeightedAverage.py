import dataclasses
import json
import re
from dataclasses import dataclass, field

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum as spark_sum, col, when, lit


class WeightedAverage(MacroSpec):
    name: str = "WeightedAverage"
    projectName: str = "prophecy_basics"
    category: str = "Transform"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]
    dependsOnUpstreamSchema: bool = True

    @dataclass(frozen=True)
    class WeightedAverageProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        valueFieldColumn: str = ""
        weightFieldColumn: str = ""
        outputFieldName: str = "WeightedAverage"
        groupByColumnNames: List[str] = field(default_factory=list)

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
        return Dialog("WeightedAverage").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer().addElement(
                        Step().addElement(
                            StackLayout(height="100%")
                            .addElement(
                                TitleElement("Value Field (Numeric)")
                            )
                            .addElement(
                                SchemaColumnsDropdown("")
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("valueFieldColumn")
                            )
                            .addElement(
                                TitleElement("Weight Field (Numeric)")
                            )
                            .addElement(
                                SchemaColumnsDropdown("")
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("weightFieldColumn")
                            )
                            .addElement(
                                TextBox("Output Field Name")
                                .bindPlaceholder("WeightedAverage")
                                .bindProperty("outputFieldName")
                            )
                            .addElement(
                                TitleElement("Grouping Fields (Optional)")
                            )
                            .addElement(
                                SchemaColumnsDropdown("")
                                .withMultipleSelection()
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("groupByColumnNames")
                            )
                        )
                    )
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(WeightedAverage, self).validate(context, component)

        if not (component.properties.valueFieldColumn or "").strip():
            diagnostics.append(
                Diagnostic(
                    "properties.valueFieldColumn",
                    "Select a value field.",
                    SeverityLevelEnum.Error,
                )
            )

        if not (component.properties.weightFieldColumn or "").strip():
            diagnostics.append(
                Diagnostic(
                    "properties.weightFieldColumn",
                    "Select a weight field.",
                    SeverityLevelEnum.Error,
                )
            )

        if not (component.properties.outputFieldName or "").strip():
            diagnostics.append(
                Diagnostic(
                    "properties.outputFieldName",
                    "Output field name is required.",
                    SeverityLevelEnum.Error,
                )
            )

        try:
            schema_js = json.loads(component.properties.schema or "[]")
        except Exception:
            schema_js = []
        schema_cols_lower = set(f["name"].lower() for f in schema_js)

        if component.properties.valueFieldColumn and component.properties.valueFieldColumn.lower() not in schema_cols_lower:
            diagnostics.append(
                Diagnostic(
                    "properties.valueFieldColumn",
                    f"Value field is not present in input schema.",
                    SeverityLevelEnum.Error,
                )
            )

        if component.properties.weightFieldColumn and component.properties.weightFieldColumn.lower() not in schema_cols_lower:
            diagnostics.append(
                Diagnostic(
                    "properties.weightFieldColumn",
                    f"Weight field is not present in input schema.",
                    SeverityLevelEnum.Error,
                )
            )

        if len(component.properties.groupByColumnNames) > 0:
            missing = [
                c for c in component.properties.groupByColumnNames
                if c.lower() not in schema_cols_lower
            ]
            if missing:
                diagnostics.append(
                    Diagnostic(
                        "properties.groupByColumnNames",
                        f"Selected columns {missing} are not present in input schema.",
                        SeverityLevelEnum.Error,
                    )
                )

        return diagnostics

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

    def apply(self, props: WeightedAverageProperties) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"

        def safe_str(val):
            if val is None or val == "":
                return "''"
            if isinstance(val, list):
                return str(val)
            return f"'{val}'"

        arguments = [
            str(props.relation_name),
            safe_str(props.valueFieldColumn),
            safe_str(props.weightFieldColumn),
            safe_str(props.outputFieldName),
            safe_str(props.groupByColumnNames),
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        return WeightedAverage.WeightedAverageProperties(
            relation_name=json.loads(parametersMap.get('relation_name').replace("'", '"')),
            schema=parametersMap.get("schema"),
            valueFieldColumn=(parametersMap.get("valueFieldColumn") or "''").lstrip("'").rstrip("'"),
            weightFieldColumn=(parametersMap.get("weightFieldColumn") or "''").lstrip("'").rstrip("'"),
            outputFieldName=(parametersMap.get("outputFieldName") or "''").lstrip("'").rstrip("'") or "WeightedAverage",
            groupByColumnNames=json.loads(
                parametersMap.get("groupByColumnNames", "[]").replace("'", '"')
            ),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("valueFieldColumn", str(properties.valueFieldColumn)),
                MacroParameter("weightFieldColumn", str(properties.weightFieldColumn)),
                MacroParameter("outputFieldName", str(properties.outputFieldName)),
                MacroParameter("groupByColumnNames", json.dumps(properties.groupByColumnNames)),
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
        value_col = self.props.valueFieldColumn
        weight_col = self.props.weightFieldColumn
        output_col = (self.props.outputFieldName or "").strip() or "WeightedAverage"
        group_cols = self.props.groupByColumnNames

        sum_val_weight = spark_sum(col(value_col) * col(weight_col))
        sum_weight = spark_sum(col(weight_col))
        weighted_avg_expr = when(sum_weight != 0, sum_val_weight / sum_weight).otherwise(lit(None)).alias(output_col)

        if group_cols:
            return in0.groupBy(*[col(c) for c in group_cols]).agg(weighted_avg_expr)
        else:
            return in0.agg(weighted_avg_expr)
