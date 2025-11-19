import dataclasses
import json
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class GenerateRows(MacroSpec):
    name: str = "GenerateRows"
    projectName: str = "prophecy_basics"
    category: str = "Prepare"
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]


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
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # generate the actual macro call given the component's
        resolved_macro_name = f"{self.projectName}.{self.name}"
        
        def safe_str(val):
            """Safely convert a value to a SQL string literal, handling None and empty strings"""
            if val is None or val == "":
                return "''"
            if isinstance(val, str):
                # Escape single quotes for SQL string literals ('' represents a single quote in SQL)
                escaped = val.replace("'", "''")
                return f"'{escaped}'"
            return f"'{str(val)}'"
        
        arguments = [
            safe_str(table_name),  # relation_name - must be present even if empty
            safe_str(props.init_expr),
            safe_str(props.condition_expr),
            safe_str(props.loop_expr),
            safe_str(props.column_name),
            str(props.max_rows),  # max_rows is required (validated in validate())
            safe_str(props.force_mode)
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
            init_expr=p.get('init_expr'),  # Use None if empty string
            condition_expr=p.get('condition_expr'),  # Use None if empty string
            loop_expr=p.get('loop_expr'),  # Use None if empty string
            column_name=p.get('column_name'),  # Use None if empty string
            max_rows=p.get('max_rows'),  # Use None if empty string
            force_mode=p.get('force_mode')  # Default to 'recursive'
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("init_expr", str(properties.init_expr)),
                MacroParameter("condition_expr", str(properties.condition_expr)),
                MacroParameter("loop_expr", str(properties.loop_expr)),
                MacroParameter("column_name", str(properties.column_name)),
                MacroParameter("max_rows", str(properties.max_rows)),
                MacroParameter("force_mode", str(properties.force_mode))
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)

    def applyPython(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
        init_expr = self.props.init_expr
        condition_expr = self.props.condition_expr
        loop_expr = self.props.loop_expr
        column_name = self.props.column_name
        max_rows = int(self.props.max_rows)
        
        # Internal column name for the generated value
        internal_col = f"__gen_{column_name.replace(' ', '_')}"
        
        # Handle None input - create empty DataFrame with empty columns list
        if in0 is None:
            in0 = spark.createDataFrame([], StructType([]))
        
        # Create payload struct if input columns exist (matching SQL macro: struct(alias.*) as payload)
        # If no columns, use spark.range(1) as base (matching SQL macro {% else %} branch)
        if in0.columns:
            payload_struct = F.struct(*[F.col(c) for c in in0.columns]).alias("payload")
            base = in0.select(payload_struct)
        else:
            payload_struct = F.struct().alias("payload")
            base = spark.range(1).select(payload_struct)
        
        # Base case: one row per input record with initial value (matching SQL macro base case)
        base_with_init = base.select(
            F.col("payload"),
            F.expr(str(init_expr).replace(column_name, internal_col)).alias(internal_col)
        )
        
        # Generate values using sequence and transform
        # NOTE: This uses a linear formula which only works for linear progressions (value + k, value - k)
        # For non-linear expressions (value * 2, value * value), this will produce incorrect results
        # Extract step: loop_expr(initial) - initial
        # For loop_expr = "value + 1" with init=1: step = (1+1) - 1 = 1
        loop_expr_with_init = str(loop_expr).replace(column_name, internal_col)
        step_expr = f"({loop_expr_with_init}) - {internal_col}"
        
        # Generate array of values: init + (i-1) * step for each iteration
        # Also track iteration number to filter by max_rows
        result = base_with_init.select(
            col("payload"),
            explode(
                expr(f"""
                    transform(
                        sequence(1, {max_rows}),
                        i -> struct(
                            {internal_col} + (i - 1) * ({step_expr}) as {internal_col},
                            i as _iter
                        )
                    )
                """)
            ).alias("_gen")
        ).select(
            col("payload"),
            col("_gen._iter").alias("_iter"),
            col(f"_gen.{internal_col}").alias(internal_col)
        )
        
        # Filter where condition is true & iteration < max_rows (matching SQL macro's WHERE clauses)
        filtered = result.filter(
            (col("_iter") <= max_rows) &
            expr(str(condition_expr).replace(column_name, internal_col))
        )
        
        # Expand payload and select final columns (matching SQL macro's payload.*)
        # If no input columns, payload will be empty struct, so only select generated column
        payload_cols = [col(f"payload.{c}").alias(c) for c in in0.columns]
        result = filtered.select(
            *payload_cols,
            col(internal_col).alias(column_name)
        )
        
        return result
