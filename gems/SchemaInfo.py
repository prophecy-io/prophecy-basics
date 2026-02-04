import dataclasses
import json
from typing import List

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType


class SchemaInfo(MacroSpec):
    name: str = "SchemaInfo"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        ProviderTypeEnum.BigQuery,
        # ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.ProphecyManaged,
    ]
    dependsOnUpstreamSchema: bool = True

    @dataclass(frozen=True)
    class SchemaInfoProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""

    def dialog(self) -> Dialog:
        return Dialog("Schema Info")

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
        schema_json = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = []
        for field in schema_json["fields"]:
            metadata = field.get("metadata", {}) or {}
            fields_array.append(
                {
                    "name": field["name"],
                    "dataType": field["dataType"]["type"],
                    "size": metadata.get("size", 4),
                    "description": "",
                    "source": "",
                    "scale": None,
                }
            )

        new_props = dataclasses.replace(
            newState.properties,
            relation_name=self.get_relation_names(newState, context),
            schema=json.dumps(fields_array),
        )
        return newState.bindProperties(new_props)

    def apply(self, props: SchemaInfoProperties) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [str(props.relation_name), props.schema]
        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        pm = self.convertToParameterMap(properties.parameters)
        return SchemaInfo.SchemaInfoProperties(
            relation_name=json.loads(pm.get("relation_name", "[]").replace("'", '"')),
            schema=pm.get("schema", ""),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", properties.schema),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        new_props = dataclasses.replace(
            component.properties,
            relation_name=self.get_relation_names(component, context),
        )
        return component.bindProperties(new_props)

    def applyPython(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
        schema_info = []
        for field in in0.schema.fields:
            field_size = field.metadata.get("size", None)
            if field_size is None:
                field_size = 4
            schema_info.append(
                (
                    field.name,
                    field.dataType.typeName(),
                    field_size,
                    "",
                    "",
                    None,
                )
            )

        schema = StructType(
            [
                StructField("Name", StringType(), nullable=True),
                StructField("Type", StringType(), nullable=True),
                StructField("Size", IntegerType(), nullable=True),
                StructField("Description", StringType(), nullable=True),
                StructField("Source", StringType(), nullable=True),
                StructField("Scale", IntegerType(), nullable=True),
            ]
        )

        return spark.createDataFrame(
            spark.sparkContext.parallelize([Row(*row) for row in schema_info]), schema
        )
