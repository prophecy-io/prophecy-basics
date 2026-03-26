import dataclasses
import json

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession, DataFrame
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
        replaceIncomingType: str = "null_val"
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
            .addOption("Null", "null_val")
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
            json.dumps(props.columnNames),
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
            replaceIncomingType=parametersMap.get("replaceIncomingType", "null_val").strip("'"),
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

    def applyPython(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
        def is_integral(dt) -> bool:
            return isinstance(dt, (ByteType, ShortType, IntegerType, LongType))

        def is_numeric(dt) -> bool:
            return isinstance(
                dt,
                (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType),
            )

        def parse_lit(s: str, dt):
            if s is None:
                return None
            t = str(s).strip()
            if dt is None:
                try:
                    return float(t)
                except (ValueError, TypeError):
                    return None
            if is_integral(dt):
                try:
                    return int(float(t))
                except (ValueError, TypeError):
                    return None
            if is_numeric(dt):
                try:
                    return float(t)
                except (ValueError, TypeError):
                    return None
            return t

        def coerce_repl(repl, dt):
            if repl is None or not is_integral(dt):
                return repl
            try:
                if isinstance(repl, bool):
                    return None
                return int(repl)
            except (ValueError, TypeError, OverflowError):
                return None

        def replacement_value(df, col_name, rw_type, rw_user, inc_type, inc_user, dt):
            c = col(col_name)
            if inc_type == "null_val":
                filter_expr = c.isNotNull()
            else:
                uv = parse_lit(inc_user, dt)
                filter_expr = (
                    (c.isNull()) | (c != lit(uv)) if uv is not None else c.isNotNull()
                )
            filtered = df.filter(filter_expr)
            repl = None
            if rw_type == "average":
                row = filtered.agg(mean(c).alias("r")).first()
                repl = row["r"] if row and row["r"] is not None else None
            elif rw_type == "median":
                quantiles = filtered.stat.approxQuantile(col_name, [0.5], 0.01)
                repl = quantiles[0] if quantiles else None
            elif rw_type == "mode":
                mode_row = filtered.groupBy(c).count().orderBy(desc("count")).first()
                repl = mode_row[0] if mode_row else None
            elif rw_type == "user":
                repl = parse_lit(rw_user, dt)
            return coerce_repl(repl, dt)

        column_names = self.props.columnNames
        replace_incoming_type = self.props.replaceIncomingType
        incoming_user_value = self.props.incomingUserValue
        replace_with_type = self.props.replaceWithType
        replace_with_user_value = self.props.replaceWithUserValue
        include_indicator = self.props.includeImputedIndicator
        output_separate = self.props.outputImputedAsSeparateField
        from pyspark.sql.functions import col, lit, when

        if not column_names:
            return in0

        exprs = []
        for cname in in0.columns:
            if cname not in column_names:
                exprs.append(col(cname))
                continue

            dtype = next(
                (f.dataType for f in in0.schema.fields if f.name == cname), None
            )

            repl = replacement_value(
                in0,
                cname,
                replace_with_type,
                replace_with_user_value,
                replace_incoming_type,
                incoming_user_value,
                dtype,
            )
            repl_lit = lit(repl) if repl is not None else lit(None)

            if replace_incoming_type == "null_val":
                is_imputed = col(cname).isNull()
            else:
                uv = parse_lit(incoming_user_value, dtype)
                is_imputed = (
                    (col(cname) == lit(uv)) if uv is not None else lit(False)
                )

            base_imputed = when(is_imputed, repl_lit).otherwise(col(cname))
            imputed_typed = (
                base_imputed.cast(dtype) if dtype is not None else base_imputed
            )

            if output_separate:
                exprs.append(col(cname))
                exprs.append(imputed_typed.alias(cname + "_ImputedValue"))
                if include_indicator:
                    exprs.append(
                        when(is_imputed, 1).otherwise(0).alias(cname + "_Indicator")
                    )
            else:
                exprs.append(imputed_typed.alias(cname))
                if include_indicator:
                    exprs.append(
                        when(is_imputed, 1).otherwise(0).alias(cname + "_Indicator")
                    )

        return in0.select(*exprs)
