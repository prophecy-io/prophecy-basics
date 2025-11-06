import dataclasses
import json

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
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
        # ProviderTypeEnum.ProphecyManaged
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
        arguments = [
            "'" + table_name + "'",
            "'" + str(props.init_expr) + "'",
            "'" + str(props.condition_expr) + "'",
            "'" + str(props.loop_expr) + "'",
            "'" + str(props.column_name) + "'",
            str(props.max_rows),
            "'" + str(props.force_mode) + "'"
        ]

        params = ",".join([param for param in arguments])
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
            new_field_name=p.get('init_expr'),
            start_expr=p.get('condition_expr'),
            end_expr=p.get('loop_expr'),
            step_expr=p.get('column_name'),
            data_type=p.get('max_rows'),
            interval_unit=p.get('force_mode')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("init_expr", properties.init_expr),
                MacroParameter("condition_expr", properties.condition_expr),
                MacroParameter("loop_expr", properties.loop_expr),
                MacroParameter("column_name", properties.v),
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

    def applyPython(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
        import re
        
        init_expr = self.props.init_expr or "1"
        condition_expr = self.props.condition_expr or "value <= 10"
        loop_expr = self.props.loop_expr or "value + 1"
        column_name = self.props.column_name or "value"
        max_rows = int(self.props.max_rows) if self.props.max_rows else 100000
        
        def to_num(s):
            try:
                return float(s) if '.' in str(s) else int(s)
            except (ValueError, TypeError):
                return None
        
        def build_expr(expr, ref_col):
            if not expr:
                return F.lit(1)
            return F.expr(str(expr).replace(column_name, ref_col))
        
        # Check if input DataFrame has data
        has_input = in0 is not None and in0.count() > 0
        
        # Extract numeric values for optimization
        init_val = to_num(init_expr)
        step_match = re.search(r'value\s*\+\s*(\d+(?:\.\d+)?)', loop_expr)
        cond_match = re.search(r'value\s*<=\s*(\d+(?:\.\d+)?)', condition_expr)
        
        # Optimized path: simple numeric case using F.sequence()
        if init_val is not None and step_match and cond_match:
            step_val = float(step_match.group(1))
            end_val = to_num(cond_match.group(1))
            if end_val is not None:
                seq = F.sequence(F.lit(init_val), F.lit(end_val), F.lit(step_val))
                base_df = in0 if has_input else spark.range(1)
                if has_input:
                    result = base_df.select(
                        *[F.col(c) for c in in0.columns],
                        F.explode(seq).alias(column_name)
                    ).filter(F.col(column_name) <= end_val)
                else:
                    result = base_df.select(
                        F.explode(seq).alias(column_name)
                    ).filter(F.col(column_name) <= end_val)
                return result
        
        # General case: build expressions and use DataFrame operations
        internal_col = f"__gen_{column_name.replace(' ', '_')}"
        
        # Create base with initial value
        init_col = build_expr(init_expr, internal_col)
        if has_input:
            col_list = [F.col(c) for c in in0.columns]
            base = in0.select(*col_list, init_col.alias(internal_col))
        else:
            base = spark.range(1).select(init_col.alias(internal_col))
        
        # Try to extract step and end values for F.sequence() optimization
        step_match = re.search(r'\+?\s*(\d+(?:\.\d+)?)', loop_expr)
        cond_match = re.search(r'<=?\s*(\d+(?:\.\d+)?)', condition_expr)
        step_val = float(step_match.group(1)) if step_match else None
        end_val = to_num(cond_match.group(1)) if cond_match else None
        
        if step_val and end_val:
            seq = F.sequence(F.col(internal_col), F.lit(end_val), F.lit(step_val))
            cond = build_expr(condition_expr, column_name)
            result = base.select(
                *[F.col(c) for c in base.columns if c != internal_col],
                F.explode(seq).alias(column_name)
            ).filter(cond)
        else:
            # Generate iterations using cross join
            iterations = spark.range(max_rows).select((F.col("id") + 1).alias("_iter"))
            expanded = base.crossJoin(iterations)
            
            # Calculate value at each iteration
            loop = build_expr(loop_expr, internal_col)
            cond = build_expr(condition_expr, internal_col)
            
            if step_val:
                value_col = (F.col(internal_col) + (F.col("_iter") - 1) * F.lit(step_val)).alias(column_name)
            else:
                value_col = loop.alias(column_name)
            
            result = expanded.select(
                *[F.col(c) for c in base.columns if c != internal_col],
                value_col
            ).filter(cond).drop("_iter")
        
        return result
