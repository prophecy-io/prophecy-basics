import dataclasses
import json

import re
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class Finance(MacroSpec):
    name: str = "Finance"
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
    class FinanceProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        functionType: str = "FV"
        outputColumn: str = "finance_result"
        rateCol: str = ""
        nperCol: str = ""
        pmtCol: str = ""
        pvCol: str = ""
        fvCol: str = ""
        principalCol: str = ""
        beginValueCol: str = ""
        endValueCol: str = ""
        periodsCol: str = ""
        nominalRateCol: str = ""
        effectRateCol: str = ""
        nperyCol: str = ""
        financeRateCol: str = ""
        reinvestRateCol: str = ""
        loBoundCol: str = ""
        hiBoundCol: str = ""
        nIterCol: str = ""
        valueColumns: List[str] = field(default_factory=list)
        dateColumns: List[str] = field(default_factory=list)
        paymentType: str = "0"
        dateDiffStyle: str = "databricks"
        excludeKeyword: str = "EXCLUDE"

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
        def cond(fnval, *elements):
            stack = StackLayout(gap="1rem")
            for e in elements:
                stack = stack.addElement(e)
            return (Condition()
                    .ifEqual(PropExpr("component.properties.functionType"), StringExpr(fnval))
                    .then(stack))

        def colpick(label, prop):
            return (StackLayout(height="100%")
                    .addElement(TitleElement(label))
                    .addElement(
                        ExpressionBox(ignoreTitle=True, language="sql")
                        .bindPlaceholder("Column name or a value/expression, e.g. rate_col or 0.05")
                        .withSchemaSuggestions()
                        .bindProperty(prop)
                    ))

        def colsdd(title, prop):
            return (StackLayout(height="100%")
                    .addElement(TitleElement(title))
                    .addElement(
                        SchemaColumnsDropdown("", appearance="minimal")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty(prop)
                    ))

        def tb(label, ph, prop):
            return TextBox(label).bindPlaceholder(ph).bindProperty(prop)

        date_style = (SelectBox("Date Diff Dialect")
                      .addOption("Databricks", "databricks")
                      .addOption("Snowflake", "snowflake")
                      .addOption("BigQuery", "bigquery")
                      .addOption("ANSI (date subtraction)", "ansi")
                      .bindProperty("dateDiffStyle"))

        def solver_fields():
            return [
                colpick("Lower bound column", "loBoundCol"),
                colpick("Upper bound column", "hiBoundCol"),
                colpick("Iterations column", "nIterCol"),
                (SelectBox("SELECT * keyword")
                 .addOption("EXCLUDE (Databricks/Snowflake/DuckDB)", "EXCLUDE")
                 .addOption("EXCEPT (BigQuery)", "EXCEPT")
                 .bindProperty("excludeKeyword")),
            ]

        return Dialog("Finance").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(gap="1rem", height="100%")
                .addElement(
                    SelectBox("Finance Function")
                    .addOption("Compound Annual Growth Rate (CAGR)", "CAGR")
                    .addOption("Effective Annual Interest Rate (EffectiveRate)", "EffectiveRate")
                    .addOption("Future Value (FV)", "FV")
                    .addOption("Future Value Schedule (FVSchedule)", "FVSchedule")
                    .addOption("Internal Rate of Return (IRR)", "IRR")
                    .addOption("Modified Internal Rate of Return (MIRR)", "MIRR")
                    .addOption("Modified Calendar-Aligned Internal Rate of Return (MXIRR)", "MXIRR")
                    .addOption("Nominal Annual Interest Rate (NominalRate)", "NominalRate")
                    .addOption("Number of Compounding Periods (NPER)", "NPER")
                    .addOption("Net Present Value (NPV)", "NPV")
                    .addOption("Periodic Payment for an Annuity/Loan (PMT)", "PMT")
                    .addOption("Present Value (PV)", "PV")
                    .addOption("Interest Rate per Period for an Annuity (Rate)", "Rate")
                    .addOption("Calendar-Aligned Internal Rate of Return (XIRR)", "XIRR")
                    .addOption("Calendar-Aligned Net Present Value (XNPV)", "XNPV")
                    .bindProperty("functionType")
                )
                .addElement(tb("Output Column Name", "finance_result", "outputColumn"))

                .addElement(cond("CAGR",
                    colpick("Beginning Value column", "beginValueCol"),
                    colpick("Ending Value column", "endValueCol"),
                    colpick("Periods column", "periodsCol"),
                ))
                .addElement(cond("EffectiveRate",
                    colpick("Nominal Rate column", "nominalRateCol"),
                    colpick("Compounding Periods / Year column", "nperyCol"),
                ))
                .addElement(cond("NominalRate",
                    colpick("Effective Rate column", "effectRateCol"),
                    colpick("Compounding Periods / Year column", "nperyCol"),
                ))
                .addElement(cond("FV",
                    colpick("Rate column", "rateCol"),
                    colpick("Number of Periods column", "nperCol"),
                    colpick("Payment column", "pmtCol"),
                    colpick("Present Value column", "pvCol"),
                    tb("Type (0 end, 1 begin)", "0", "paymentType"),
                ))
                .addElement(cond("PV",
                    colpick("Rate column", "rateCol"),
                    colpick("Number of Periods column", "nperCol"),
                    colpick("Payment column", "pmtCol"),
                    colpick("Future Value column", "fvCol"),
                    tb("Type (0 end, 1 begin)", "0", "paymentType"),
                ))
                .addElement(cond("PMT",
                    colpick("Rate column", "rateCol"),
                    colpick("Number of Periods column", "nperCol"),
                    colpick("Present Value column", "pvCol"),
                    colpick("Future Value column", "fvCol"),
                    tb("Type (0 end, 1 begin)", "0", "paymentType"),
                ))
                .addElement(cond("NPER",
                    colpick("Rate column", "rateCol"),
                    colpick("Payment column", "pmtCol"),
                    colpick("Present Value column", "pvCol"),
                    colpick("Future Value column", "fvCol"),
                    tb("Type (0 end, 1 begin)", "0", "paymentType"),
                ))
                .addElement(cond("NPV",
                    colpick("Discount Rate column", "rateCol"),
                    colsdd("Select cash-flow columns (period 1..n)", "valueColumns"),
                ))
                .addElement(cond("XNPV",
                    colpick("Discount Rate column", "rateCol"),
                    colsdd("Select cash-flow columns", "valueColumns"),
                    colsdd("Select date columns (aligned to cash flows)", "dateColumns"),
                    date_style,
                ))
                .addElement(cond("FVSchedule",
                    colpick("Principal column", "principalCol"),
                    colsdd("Select rate-schedule columns", "valueColumns"),
                ))
                .addElement(cond("IRR",
                    colsdd("Select cash-flow columns (period 0..n)", "valueColumns"),
                    *solver_fields(),
                ))
                .addElement(cond("MIRR",
                    colsdd("Select cash-flow columns (period 0..n)", "valueColumns"),
                    colpick("Finance Rate column", "financeRateCol"),
                    colpick("Reinvest Rate column", "reinvestRateCol"),
                ))
                .addElement(cond("MXIRR",
                    colsdd("Select cash-flow columns", "valueColumns"),
                    colsdd("Select date columns (aligned)", "dateColumns"),
                    colpick("Finance Rate column", "financeRateCol"),
                    colpick("Reinvest Rate column", "reinvestRateCol"),
                    date_style,
                ))
                .addElement(cond("Rate",
                    colpick("Number of Periods column", "nperCol"),
                    colpick("Payment column", "pmtCol"),
                    colpick("Present Value column", "pvCol"),
                    colpick("Future Value column", "fvCol"),
                    tb("Type (0 end, 1 begin)", "0", "paymentType"),
                    *solver_fields(),
                ))
                .addElement(cond("XIRR",
                    colsdd("Select cash-flow columns", "valueColumns"),
                    colsdd("Select date columns (aligned)", "dateColumns"),
                    date_style,
                    *solver_fields(),
                ))
            )
        )

    # Single-value (column-or-expression) fields required per function, keyed by
    # property name -> label shown in the dialog.
    REQUIRED_FIELDS_BY_FUNCTION = {
        "CAGR": [("beginValueCol", "Beginning Value column"), ("endValueCol", "Ending Value column"), ("periodsCol", "Periods column")],
        "EffectiveRate": [("nominalRateCol", "Nominal Rate column"), ("nperyCol", "Compounding Periods / Year column")],
        "NominalRate": [("effectRateCol", "Effective Rate column"), ("nperyCol", "Compounding Periods / Year column")],
        "FV": [("rateCol", "Rate column"), ("nperCol", "Number of Periods column"), ("pmtCol", "Payment column"), ("pvCol", "Present Value column")],
        "PV": [("rateCol", "Rate column"), ("nperCol", "Number of Periods column"), ("pmtCol", "Payment column"), ("fvCol", "Future Value column")],
        "PMT": [("rateCol", "Rate column"), ("nperCol", "Number of Periods column"), ("pvCol", "Present Value column"), ("fvCol", "Future Value column")],
        "NPER": [("rateCol", "Rate column"), ("pmtCol", "Payment column"), ("pvCol", "Present Value column"), ("fvCol", "Future Value column")],
        "NPV": [("rateCol", "Discount Rate column")],
        "XNPV": [("rateCol", "Discount Rate column")],
        "FVSchedule": [("principalCol", "Principal column")],
        "MIRR": [("financeRateCol", "Finance Rate column"), ("reinvestRateCol", "Reinvest Rate column")],
        "MXIRR": [("financeRateCol", "Finance Rate column"), ("reinvestRateCol", "Reinvest Rate column")],
        "Rate": [("nperCol", "Number of Periods column"), ("pmtCol", "Payment column"), ("pvCol", "Present Value column"), ("fvCol", "Future Value column")],
    }

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super().validate(context, component)
        p = component.properties
        ft = p.functionType

        def err(field, msg):
            diagnostics.append(Diagnostic(f"properties.{field}", msg, SeverityLevelEnum.Error))

        if not (p.outputColumn or "").strip():
            err("outputColumn", "Output column name cannot be empty.")

        list_fns = ("NPV", "XNPV", "FVSchedule", "IRR", "MIRR", "MXIRR", "XIRR")
        if ft in list_fns and len(p.valueColumns) == 0:
            err("valueColumns", f"{ft} requires at least one selected column.")

        date_fns = ("XNPV", "XIRR", "MXIRR")
        if ft in date_fns:
            if len(p.dateColumns) == 0:
                err("dateColumns", f"{ft} requires at least one selected date column.")
            elif len(p.valueColumns) != len(p.dateColumns):
                err("dateColumns", "Select the same number of date columns as cash-flow columns.")

        if ft in ("MIRR", "MXIRR") and 0 < len(p.valueColumns) < 2:
            err("valueColumns", f"{ft} needs at least two cash-flow columns.")

        for prop, label in self.REQUIRED_FIELDS_BY_FUNCTION.get(ft, []):
            if not (getattr(p, prop) or "").strip():
                err(prop, f"{label} is required for {ft}.")

        return diagnostics

    def _extract_schema(self, component: Component) -> str:
        try:
            raw = component.ports.inputs[0].schema
            schema = json.loads(raw) if isinstance(raw, str) else (raw or {})
            fields_array = [
                {"name": f["name"], "dataType": f["dataType"]["type"]}
                for f in schema.get("fields", [])
            ]
            return json.dumps(fields_array)
        except Exception:
            return ""

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        relation_name = self.get_relation_names(newState, context)
        schema = self._extract_schema(newState)
        newProperties = dataclasses.replace(
            newState.properties,
            relation_name=relation_name,
            schema=schema,
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: FinanceProperties) -> str:
        # Generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"

        def q(val):
            s = "0" if (val is None or str(val).strip() == "") else str(val).strip()
            return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

        value_list = ",".join(props.valueColumns) if props.valueColumns else ""
        date_list = ",".join(props.dateColumns) if props.dateColumns else ""

        arguments = [
            str(props.relation_name),   # 1  relation_name (list, like CountRecords)
            q(props.functionType),
            q(props.outputColumn if props.outputColumn.strip() != "" else "finance_result"),
            q(props.rateCol),
            q(props.nperCol),
            q(props.pmtCol),
            q(props.pvCol),
            q(props.fvCol),
            q(props.paymentType),
            q(props.principalCol),
            q(value_list),
            q(date_list),
            q(props.beginValueCol),
            q(props.endValueCol),
            q(props.periodsCol),
            q(props.nominalRateCol),
            q(props.effectRateCol),
            q(props.nperyCol),
            q(props.financeRateCol),
            q(props.reinvestRateCol),
            q(props.loBoundCol),
            q(props.hiBoundCol),
            q(props.nIterCol),
            q(props.dateDiffStyle),
            q(props.excludeKeyword),
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        m = self.convertToParameterMap(properties.parameters)

        def jload(key):
            raw = m.get(key, '[]')
            try:
                return json.loads(raw.replace("'", '"')) if raw else []
            except Exception:
                return []

        def clean(v, default="0"):
            if v is None or str(v).strip() == "" or str(v).strip() == "''":
                return default
            return str(v).lstrip("'").rstrip("'")

        def cleancol(v):
            if v is None:
                return ""
            return str(v).lstrip("'").rstrip("'")

        return Finance.FinanceProperties(
            relation_name=jload('relation_name'),
            schema=m.get('schema', ''),
            functionType=clean(m.get('functionType'), 'FV'),
            outputColumn=clean(m.get('outputColumn'), 'finance_result'),
            rateCol=cleancol(m.get('rateCol')),
            nperCol=cleancol(m.get('nperCol')),
            pmtCol=cleancol(m.get('pmtCol')),
            pvCol=cleancol(m.get('pvCol')),
            fvCol=cleancol(m.get('fvCol')),
            principalCol=cleancol(m.get('principalCol')),
            beginValueCol=cleancol(m.get('beginValueCol')),
            endValueCol=cleancol(m.get('endValueCol')),
            periodsCol=cleancol(m.get('periodsCol')),
            nominalRateCol=cleancol(m.get('nominalRateCol')),
            effectRateCol=cleancol(m.get('effectRateCol')),
            nperyCol=cleancol(m.get('nperyCol')),
            financeRateCol=cleancol(m.get('financeRateCol')),
            reinvestRateCol=cleancol(m.get('reinvestRateCol')),
            loBoundCol=cleancol(m.get('loBoundCol')),
            hiBoundCol=cleancol(m.get('hiBoundCol')),
            nIterCol=cleancol(m.get('nIterCol')),
            valueColumns=jload('valueColumns'),
            dateColumns=jload('dateColumns'),
            paymentType=clean(m.get('paymentType'), '0'),
            dateDiffStyle=clean(m.get('dateDiffStyle'), 'databricks'),
            excludeKeyword=clean(m.get('excludeKeyword'), 'EXCLUDE'),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("functionType", str(properties.functionType)),
                MacroParameter("outputColumn", str(properties.outputColumn)),
                MacroParameter("rateCol", str(properties.rateCol)),
                MacroParameter("nperCol", str(properties.nperCol)),
                MacroParameter("pmtCol", str(properties.pmtCol)),
                MacroParameter("pvCol", str(properties.pvCol)),
                MacroParameter("fvCol", str(properties.fvCol)),
                MacroParameter("principalCol", str(properties.principalCol)),
                MacroParameter("beginValueCol", str(properties.beginValueCol)),
                MacroParameter("endValueCol", str(properties.endValueCol)),
                MacroParameter("periodsCol", str(properties.periodsCol)),
                MacroParameter("nominalRateCol", str(properties.nominalRateCol)),
                MacroParameter("effectRateCol", str(properties.effectRateCol)),
                MacroParameter("nperyCol", str(properties.nperyCol)),
                MacroParameter("financeRateCol", str(properties.financeRateCol)),
                MacroParameter("reinvestRateCol", str(properties.reinvestRateCol)),
                MacroParameter("loBoundCol", str(properties.loBoundCol)),
                MacroParameter("hiBoundCol", str(properties.hiBoundCol)),
                MacroParameter("nIterCol", str(properties.nIterCol)),
                MacroParameter("valueColumns", json.dumps(properties.valueColumns)),
                MacroParameter("dateColumns", json.dumps(properties.dateColumns)),
                MacroParameter("paymentType", str(properties.paymentType)),
                MacroParameter("dateDiffStyle", str(properties.dateDiffStyle)),
                MacroParameter("excludeKeyword", str(properties.excludeKeyword)),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)
        schema = self._extract_schema(component)
        newProperties = dataclasses.replace(
            component.properties,
            relation_name=relation_name,
            schema=schema,
        )
        return component.bindProperties(newProperties)
