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
        init_expr = self.props.init_expr
        condition_expr = self.props.condition_expr
        loop_expr = self.props.loop_expr
        column_name = self.props.column_name
        max_rows = int(self.props.max_rows) if self.props.max_rows else 100000

        # Since PySpark doesn't support recursive CTEs like SQL,
        # we simulate the recursive behavior using Python UDF
        
        # Parse expressions to extract column references
        # For init_expr, condition_expr, loop_expr - these can reference columns or be literals
        
        def generate_values(init_val, max_iterations=100000):
            """Generate values based on recursive logic"""
            values = []
            current = init_val
            iteration = 0
            
            # Simple evaluation of condition_expr - assumes it references 'value'
            # This is a simplified parser - full implementation would need proper expression parsing
            try:
                # Parse condition_expr to extract the limit value
                # For expressions like "value <= 10" or "value < payload.col"
                condition_str = condition_expr.replace("value", str(current))
                # Simple numeric limit extraction
                import re
                matches = re.findall(r'<=?\s*(\d+)', condition_expr)
                if matches:
                    limit_val = int(matches[0])
                    operator = '<=' if '<=' in condition_expr else '<'
                else:
                    matches = re.findall(r'>=\s*(\d+)', condition_expr)
                    if matches:
                        limit_val = int(matches[0])
                        operator = '>='
                    else:
                        # Default: use max_rows as limit
                        limit_val = current + max_rows
                        operator = '<='
                
                # Parse loop_expr - assumes form like "value + 1" or "value * 2"
                if '+' in loop_expr:
                    step = int(re.findall(r'\+\s*(\d+)', loop_expr)[0]) if re.findall(r'\+\s*(\d+)', loop_expr) else 1
                elif '-' in loop_expr:
                    step = -int(re.findall(r'-\s*(\d+)', loop_expr)[0]) if re.findall(r'-\s*(\d+)', loop_expr) else -1
                elif '*' in loop_expr:
                    multiplier = float(re.findall(r'\*\s*(\d+(?:\.\d+)?)', loop_expr)[0]) if re.findall(r'\*\s*(\d+(?:\.\d+)?)', loop_expr) else 1
                    step = None  # Use multiplication instead
                else:
                    step = 1
                
                # Generate values iteratively
                while iteration < max_iterations:
                    values.append(current)
                    
                    # Check condition
                    if operator == '<=':
                        if not (current <= limit_val):
                            break
                    elif operator == '<':
                        if not (current < limit_val):
                            break
                    elif operator == '>=':
                        if not (current >= limit_val):
                            break
                    
                    # Apply loop_expr
                    if step is not None:
                        current = current + step if '+' in loop_expr or '-' not in loop_expr else current + step
                    else:
                        current = current * multiplier
                    
                    iteration += 1
                    
            except Exception as e:
                # Fallback: generate simple sequence
                values = list(range(init_val, init_val + min(max_rows, 1000)))
            
            return values
        
        # Check if we have input data or standalone generation
        if in0 is not None and in0.count() > 0:
            # Generate rows per input row
            # For each row, generate the sequence of values
            
            # Create UDF to generate values
            generate_udf = F.udf(
                lambda init: generate_values(init, max_rows),
                ArrayType(IntegerType())
            )
            
            # Parse init_expr - might be a literal or column reference
            try:
                if isinstance(init_expr, (int, float)):
                    init_value = init_expr
                elif isinstance(init_expr, str):
                    if init_expr.strip().isdigit():
                        init_value = int(init_expr.strip())
                    elif init_expr.replace('.', '', 1).isdigit():
                        init_value = float(init_expr.strip())
                    else:
                        # Might be a column reference - use as is
                        init_col = F.col(init_expr)
                        result_df = in0.withColumn(
                            "_generated_values",
                            generate_udf(init_col)
                        )
                        result_df = result_df.select(
                            "*",
                            F.explode(F.col("_generated_values")).alias(column_name)
                        )
                        result_df = result_df.drop("_generated_values")
                        return result_df
                else:
                    init_value = 1
            except:
                init_value = 1
            
            # Generate values and explode
            result_df = in0.withColumn(
                "_generated_values",
                F.array([F.lit(i) for i in generate_values(init_value, max_rows)])
            )
            result_df = result_df.select(
                "*",
                F.explode(F.col("_generated_values")).alias(column_name)
            )
            result_df = result_df.drop("_generated_values")
        else:
            # Standalone generation - no input data
            values = generate_values(
                int(init_expr) if isinstance(init_expr, str) and init_expr.strip().isdigit() else (init_expr if isinstance(init_expr, (int, float)) else 1),
                max_rows
            )
            
            # Create DataFrame with generated values
            rows = [Row(**{column_name: v}) for v in values]
            result_df = spark.createDataFrame(rows)
        
        return result_df
