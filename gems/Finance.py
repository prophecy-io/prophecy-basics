import dataclasses
import json
import math

import re
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


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
        # Component wiring, not user facing.
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""

        # Used by every function.
        functionType: str = "FV"
        outputColumn: str = "finance_result"

        # Time-value-of-money inputs. Each of FV / PV / PMT / NPER solves for one
        # of these, so the function that owns a value never asks for it.
        rateCol: str = ""          # FV, PV, PMT, NPER + discount rate for NPV, XNPV
        nperCol: str = ""          # FV, PV, PMT, Rate
        pmtCol: str = ""           # FV, PV, NPER, Rate
        pvCol: str = ""            # FV, PMT, NPER, Rate
        fvCol: str = ""            # PV, PMT, NPER, Rate
        paymentType: str = "0"     # FV, PV, PMT, NPER, Rate

        # CAGR.
        beginValueCol: str = ""
        endValueCol: str = ""
        periodsCol: str = ""

        # Interest-rate conversion.
        nominalRateCol: str = ""   # EffectiveRate
        effectRateCol: str = ""    # NominalRate
        nperyCol: str = ""         # EffectiveRate, NominalRate

        # FVSchedule.
        principalCol: str = ""

        # MIRR, MXIRR.
        financeRateCol: str = ""
        reinvestRateCol: str = ""

        # Multi-column series.
        valueColumns: List[str] = field(default_factory=list)  # NPV, XNPV, FVSchedule, IRR, MIRR, MXIRR, XIRR
        dateColumns: List[str] = field(default_factory=list)   # XNPV, XIRR, MXIRR

        # Bisection solver knobs for the iterative functions: IRR, Rate, XIRR.
        # Blank means the macro's own defaults (-0.99 / 10 / 60 iterations).
        loBoundCol: str = ""
        hiBoundCol: str = ""
        nIterCol: str = ""

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

    # Help text shown under the function picker. Formulas are written with the same
    # field names the dialog shows, and every example is reproducible against the
    # matching branch in macros/Finance.sql.
    FUNCTION_TIPS = {
        "CAGR":
            "**Compound Annual Growth Rate** — the steady per-period rate that grows a starting"
            " amount into an ending amount."
            "\n"
            "- Formula: `(Ending Value / Beginning Value) ^ (1 / Periods) - 1`"
            "\n"
            "- Example: Beginning Value `10000`, Ending Value `19000`, Periods `5`"
            " gives `0.1370`, meaning 13.70% growth a year",
        "EffectiveRate":
            "**Effective Annual Interest Rate** — the rate you actually earn over a year once"
            " compounding inside the year is counted."
            "\n"
            "- Formula: `(1 + Nominal Rate / Times Compounded per Year) ^ Times Compounded per Year - 1`"
            "\n"
            "- Example: Nominal Rate `0.12` compounded monthly, so Times Compounded per Year `12`,"
            " gives `0.126825`, meaning 12.68% a year",
        "FV":
            "**Future Value** — what an investment grows to after a number of periods, counting"
            " both the starting balance and a repeating payment."
            "\n"
            "- Formula: `-(Present Value * (1 + Rate) ^ Number of Periods"
            " + Payment * (1 + Rate * Type) * ((1 + Rate) ^ Number of Periods - 1) / Rate)`"
            "\n"
            "- Example: Rate `0.05`, Number of Periods `10`, Payment `-100`, Present Value `-1000`"
            " gives `2886.68`"
            "\n"
            "- Sign convention: money you pay out is negative, money you receive is positive",
        "FVSchedule":
            "**Future Value Schedule** — grows a starting amount through a series of different"
            " rates, taking one rate from each selected column."
            "\n"
            "- Formula: `Principal * (1 + first rate) * (1 + second rate) * ...`"
            "\n"
            "- Example: Principal `1000` with rate columns holding `0.05`, `0.06` and `0.07`"
            " gives `1190.91`",
        "IRR":
            "**Internal Rate of Return** — the rate at which a series of cash flows breaks even,"
            " that is, the rate that discounts them to a total of zero."
            "\n"
            "- Formula: finds the rate where"
            " `first flow + second flow / (1 + rate) ^ 1 + third flow / (1 + rate) ^ 2 + ... = 0`"
            "\n"
            "- The first selected column is the flow at time zero, usually the initial investment"
            "\n"
            "- Example: cash-flow columns holding `-1000`, `500`, `500` and `500`"
            " gives about `0.234`, meaning a 23.4% return"
            "\n"
            "- The flows must switch between negative and positive at least once, or there is no"
            " break-even rate to find",
        "MIRR":
            "**Modified Internal Rate of Return** — like IRR, but you say what rate the money"
            " coming in earns and what rate the money going out costs."
            "\n"
            "- Formula:"
            " `(value of inflows grown at Reinvest Rate / value of outflows discounted at Finance Rate)"
            " ^ (1 / number of periods) - 1`"
            "\n"
            "- Example: cash-flow columns holding `-1000`, `400`, `500` and `600`, with Finance Rate"
            " `0.10` and Reinvest Rate `0.12`, gives about `0.1845`",
        "MXIRR":
            "**Calendar-Aligned Modified Internal Rate of Return** — MIRR that spaces the cash"
            " flows by their real dates rather than treating them as evenly spaced periods."
            "\n"
            "- Pick the same number of Date columns as Cash-flow columns, in matching order."
            " The earliest date is the starting point everything else is measured from, and a"
            " year counts as 365 days"
            "\n"
            "- Example: `-1000` dated `2024-01-01` and `1200` dated `2025-01-01`, with Finance Rate"
            " `0.10` and Reinvest Rate `0.12`, gives about `0.199`",
        "NominalRate":
            "**Nominal Annual Interest Rate** — the quoted, advertised rate that works out to a"
            " given effective rate once it compounds during the year. The reverse of EffectiveRate."
            "\n"
            "- Formula:"
            " `Times Compounded per Year * ((1 + Effective Rate) ^ (1 / Times Compounded per Year) - 1)`"
            "\n"
            "- Example: Effective Rate `0.126825` with Times Compounded per Year `12` gives `0.12`,"
            " meaning a quoted rate of 12%",
        "NPER":
            "**Number of Compounding Periods** — how many periods it takes to pay a loan off, or to"
            " reach a target balance, at a fixed rate."
            "\n"
            "- Formula: `ln((Payment * (1 + Rate * Type) - Future Value * Rate)"
            " / (Payment * (1 + Rate * Type) + Present Value * Rate)) / ln(1 + Rate)`"
            "\n"
            "- Example: Rate `0.05`, Payment `-1295.05`, Present Value `10000`, Future Value `0`"
            " gives `10` periods"
            "\n"
            "- Sign convention: money you pay out is negative, money you receive is positive",
        "NPV":
            "**Net Present Value** — what a series of future cash flows is worth in today's money."
            "\n"
            "- Formula: `first flow / (1 + Discount Rate) ^ 1 + second flow / (1 + Discount Rate) ^ 2 + ...`,"
            " one term per selected column"
            "\n"
            "- The first selected column is treated as one period in the future, not as today"
            "\n"
            "- Example: Discount Rate `0.10` with cash-flow columns holding `100`, `200` and `300`"
            " gives `481.59`",
        "PMT":
            "**Periodic Payment** — the fixed amount due each period to pay off a loan, or to reach"
            " a target balance, over a set number of periods."
            "\n"
            "- Formula: `-(Present Value * (1 + Rate) ^ Number of Periods + Future Value)"
            " / ((1 + Rate * Type) * ((1 + Rate) ^ Number of Periods - 1) / Rate)`"
            "\n"
            "- Example: Rate `0.05`, Number of Periods `10`, Present Value `10000`, Future Value `0`"
            " gives `-1295.05`, that is, you pay 1295.05 a period"
            "\n"
            "- Sign convention: money you pay out is negative, money you receive is positive",
        "PV":
            "**Present Value** — what a future amount plus a run of repeating payments is worth in"
            " today's money."
            "\n"
            "- Formula: `-(Future Value"
            " + Payment * (1 + Rate * Type) * ((1 + Rate) ^ Number of Periods - 1) / Rate)"
            " / (1 + Rate) ^ Number of Periods`"
            "\n"
            "- Example: Rate `0.05`, Number of Periods `10`, Payment `-100`, Future Value `0`"
            " gives `772.17`"
            "\n"
            "- Sign convention: money you pay out is negative, money you receive is positive",
        "Rate":
            "**Interest Rate per Period** — the rate being charged or earned, worked backwards from"
            " a known payment, balance and term."
            "\n"
            "- Formula: finds the rate where `Present Value * (1 + rate) ^ Number of Periods"
            " + Payment * (1 + rate * Type) * ((1 + rate) ^ Number of Periods - 1) / rate"
            " + Future Value = 0`"
            "\n"
            "- Example: Number of Periods `10`, Payment `-1295.05`, Present Value `10000`,"
            " Future Value `0` gives `0.05`, meaning 5% a period"
            "\n"
            "- Widen the solver bounds below if you expect a rate outside `-0.99` to `10`",
        "XIRR":
            "**Calendar-Aligned Internal Rate of Return** — IRR that spaces the cash flows by their"
            " real dates rather than treating them as evenly spaced periods."
            "\n"
            "- Pick the same number of Date columns as Cash-flow columns, in matching order."
            " The earliest date is the starting point everything else is measured from, and a"
            " year counts as 365 days"
            "\n"
            "- Example: `-1000` dated `2024-01-01` and `1100` dated `2024-12-31`, which is 365 days"
            " later, gives `0.10`, meaning a 10% annual return",
        "XNPV":
            "**Calendar-Aligned Net Present Value** — NPV that discounts each cash flow by its real"
            " date rather than by an evenly spaced period number."
            "\n"
            "- Formula: add up"
            " `each flow / (1 + Discount Rate) ^ (days after the earliest date / 365)`"
            "\n"
            "- Pick the same number of Date columns as Cash-flow columns, in matching order"
            "\n"
            "- Example: Discount Rate `0.10` with `-1000` dated `2024-01-01` and `1100` dated"
            " `2024-12-31` gives `0.00`, so the deal exactly breaks even at 10%",
    }

    def dialog(self) -> Dialog:
        def boxed(*elements):
            # Groups elements into one bordered card, like the TextToColumns dialog.
            stack = StackLayout(height="100%")
            for e in elements:
                stack = stack.addElement(e)
            return StepContainer().addElement(Step().addElement(stack))

        def when(fnval, element):
            return (Condition()
                    .ifEqual(PropExpr("component.properties.functionType"), StringExpr(fnval))
                    .then(element))

        def cond(fnval, *elements):
            # One outer border per function; the controls inside carry their own.
            return when(fnval, boxed(TitleElement("Function Parameters"), *elements))

        def tips(fnval):
            return when(fnval, AlertBox(
                variant="success",
                _children=[Markdown(self.FUNCTION_TIPS[fnval])],
            ))

        def colpick(label, prop):
            return (SchemaColumnsDropdown(label)
                    .bindSchema("component.ports.inputs[0].schema")
                    .bindProperty(prop)
                    .showErrorsFor(prop))

        def colsdd(label, prop):
            return (SchemaColumnsDropdown(label)
                    .withMultipleSelection()
                    .bindSchema("component.ports.inputs[0].schema")
                    .bindProperty(prop)
                    .showErrorsFor(prop))

        def tb(label, ph, prop):
            return TextBox(label).bindPlaceholder(ph).bindProperty(prop)

        def solver_fields():
            return [
                TitleElement("Bisection solver (optional)"),
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(tb("Lower bound", "-0.99", "loBoundCol"))
                .addColumn(tb("Upper bound", "10", "hiBoundCol"))
                .addColumn(tb("Iterations", "60", "nIterCol")),
            ]

        return Dialog("Finance").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout()
                .addElement(
                    StackLayout(height="100%")
                    .addElement(
                        boxed(
                            TitleElement("Select Finance Function"),
                            SelectBox("")
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
                            .bindProperty("functionType"),
                            *[tips(fn) for fn in self.FUNCTION_TIPS],
                        )
                    )
                    .addElement(boxed(tb("Output Column Name", "finance_result", "outputColumn")))
                    .addElement(cond("CAGR",
                        colpick("Beginning Value", "beginValueCol"),
                        colpick("Ending Value", "endValueCol"),
                        colpick("Periods", "periodsCol"),
                    ))
                    .addElement(cond("EffectiveRate",
                        colpick("Nominal Rate", "nominalRateCol"),
                        colpick("Times Compounded per Year", "nperyCol"),
                    ))
                    .addElement(cond("NominalRate",
                        colpick("Effective Rate", "effectRateCol"),
                        colpick("Times Compounded per Year", "nperyCol"),
                    ))
                    .addElement(cond("FV",
                        colpick("Rate", "rateCol"),
                        colpick("Number of Periods", "nperCol"),
                        colpick("Payment", "pmtCol"),
                        colpick("Present Value", "pvCol"),
                        tb("Type (0 = payment at period end, 1 = at start)", "0", "paymentType"),
                    ))
                    .addElement(cond("PV",
                        colpick("Rate", "rateCol"),
                        colpick("Number of Periods", "nperCol"),
                        colpick("Payment", "pmtCol"),
                        colpick("Future Value", "fvCol"),
                        tb("Type (0 = payment at period end, 1 = at start)", "0", "paymentType"),
                    ))
                    .addElement(cond("PMT",
                        colpick("Rate", "rateCol"),
                        colpick("Number of Periods", "nperCol"),
                        colpick("Present Value", "pvCol"),
                        colpick("Future Value", "fvCol"),
                        tb("Type (0 = payment at period end, 1 = at start)", "0", "paymentType"),
                    ))
                    .addElement(cond("NPER",
                        colpick("Rate", "rateCol"),
                        colpick("Payment", "pmtCol"),
                        colpick("Present Value", "pvCol"),
                        colpick("Future Value", "fvCol"),
                        tb("Type (0 = payment at period end, 1 = at start)", "0", "paymentType"),
                    ))
                    .addElement(cond("NPV",
                        colpick("Discount Rate", "rateCol"),
                        colsdd("Cash-flow columns, earliest first", "valueColumns"),
                    ))
                    .addElement(cond("XNPV",
                        colpick("Discount Rate", "rateCol"),
                        colsdd("Cash-flow columns, earliest first", "valueColumns"),
                        colsdd("Date columns, one per cash-flow column in the same order", "dateColumns"),
                    ))
                    .addElement(cond("FVSchedule",
                        colpick("Principal", "principalCol"),
                        colsdd("Rate columns, applied in order", "valueColumns"),
                    ))
                    .addElement(cond("IRR",
                        colsdd("Cash-flow columns, starting with the flow at time zero", "valueColumns"),
                        *solver_fields(),
                    ))
                    .addElement(cond("MIRR",
                        colsdd("Cash-flow columns, starting with the flow at time zero", "valueColumns"),
                        colpick("Finance Rate", "financeRateCol"),
                        colpick("Reinvest Rate", "reinvestRateCol"),
                    ))
                    .addElement(cond("MXIRR",
                        colsdd("Cash-flow columns, earliest first", "valueColumns"),
                        colsdd("Date columns, one per cash-flow column in the same order", "dateColumns"),
                        colpick("Finance Rate", "financeRateCol"),
                        colpick("Reinvest Rate", "reinvestRateCol"),
                    ))
                    .addElement(cond("Rate",
                        colpick("Number of Periods", "nperCol"),
                        colpick("Payment", "pmtCol"),
                        colpick("Present Value", "pvCol"),
                        colpick("Future Value", "fvCol"),
                        tb("Type (0 = payment at period end, 1 = at start)", "0", "paymentType"),
                        *solver_fields(),
                    ))
                    .addElement(cond("XIRR",
                        colsdd("Cash-flow columns, earliest first", "valueColumns"),
                        colsdd("Date columns, one per cash-flow column in the same order", "dateColumns"),
                        *solver_fields(),
                    ))
                )
            )
        )

    # Columns each function must have filled in, keyed by property name -> the
    # label the dialog shows for it, so error messages match what the user sees.
    REQUIRED_FIELDS_BY_FUNCTION = {
        "CAGR": [("beginValueCol", "Beginning Value"), ("endValueCol", "Ending Value"), ("periodsCol", "Periods")],
        "EffectiveRate": [("nominalRateCol", "Nominal Rate"), ("nperyCol", "Times Compounded per Year")],
        "NominalRate": [("effectRateCol", "Effective Rate"), ("nperyCol", "Times Compounded per Year")],
        "FV": [("rateCol", "Rate"), ("nperCol", "Number of Periods"), ("pmtCol", "Payment"), ("pvCol", "Present Value")],
        "PV": [("rateCol", "Rate"), ("nperCol", "Number of Periods"), ("pmtCol", "Payment"), ("fvCol", "Future Value")],
        "PMT": [("rateCol", "Rate"), ("nperCol", "Number of Periods"), ("pvCol", "Present Value"), ("fvCol", "Future Value")],
        "NPER": [("rateCol", "Rate"), ("pmtCol", "Payment"), ("pvCol", "Present Value"), ("fvCol", "Future Value")],
        "NPV": [("rateCol", "Discount Rate")],
        "XNPV": [("rateCol", "Discount Rate")],
        "FVSchedule": [("principalCol", "Principal")],
        "MIRR": [("financeRateCol", "Finance Rate"), ("reinvestRateCol", "Reinvest Rate")],
        "MXIRR": [("financeRateCol", "Finance Rate"), ("reinvestRateCol", "Reinvest Rate")],
        "Rate": [("nperCol", "Number of Periods"), ("pmtCol", "Payment"), ("pvCol", "Present Value"), ("fvCol", "Future Value")],
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
                err(prop, f"Select a column for {label}.")

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

        def quote(s):
            return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

        def q(val):
            # Numeric inputs: blank becomes a neutral 0 so the SQL stays valid.
            return quote("0" if (val is None or str(val).strip() == "") else str(val).strip())

        def qraw(val):
            # Blank is passed through so the macro applies its own default.
            return quote("" if val is None else str(val).strip())

        value_list = ",".join(props.valueColumns) if props.valueColumns else ""
        date_list = ",".join(props.dateColumns) if props.dateColumns else ""

        arguments = [
            str(props.relation_name),                                                        # relation_name
            q(props.functionType),                                                           # function_type
            q(props.outputColumn if props.outputColumn.strip() != "" else "finance_result"), # output_column
            q(props.rateCol),                                                                # rate
            q(props.nperCol),                                                                # nper
            q(props.pmtCol),                                                                 # pmt
            q(props.pvCol),                                                                  # pv
            q(props.fvCol),                                                                  # fv
            q(props.paymentType),                                                            # pay_type
            q(props.principalCol),                                                           # principal
            qraw(value_list),                                                                # value_list
            qraw(date_list),                                                                 # date_list
            q(props.beginValueCol),                                                          # begin_value
            q(props.endValueCol),                                                            # end_value
            q(props.periodsCol),                                                             # periods
            q(props.nominalRateCol),                                                         # nominal_rate
            q(props.effectRateCol),                                                          # effect_rate
            q(props.nperyCol),                                                               # npery
            q(props.financeRateCol),                                                         # finance_rate
            q(props.reinvestRateCol),                                                        # reinvest_rate
            qraw(props.loBoundCol),                                                          # lo_bound
            qraw(props.hiBoundCol),                                                          # hi_bound
            qraw(props.nIterCol),                                                            # n_iter
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

    def applyPython(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
        props = self.props
        fn = (props.functionType or "FV").strip().lower()
        out_col = (props.outputColumn or "").strip() or "finance_result"

        def num(value, blank="0"):
            """A dialog field as a numeric column.

            Fields hold either a column name or an expression, the same way the macro
            treats them, so the text is parsed rather than looked up as a name.
            """
            text = "" if value is None else str(value).strip()
            return F.expr(text or blank).cast("double")

        def nz(column):
            """Zero becomes null, so dividing by it yields null instead of failing."""
            return F.when(column == 0, F.lit(None).cast("double")).otherwise(column)

        def total(terms):
            result = F.lit(0.0)
            for term in terms:
                result = result + term
            return result

        # Powers go through math.pow rather than the ** operator: Prophecy's Python parser
        # reads ** as keyword-argument unpacking and fails to build the project when the
        # expression to its left is anything more complex than a name.
        #
        # The three solvers below stay local on purpose. A UDF that calls a module-level
        # function is shipped to the workers as a reference, which makes every worker
        # import this file and therefore the whole Prophecy package. Defined here they
        # travel by value instead, so the workers need nothing but the standard library.
        def sign(value):
            return (value > 0) - (value < 0)

        def bisect(objective, low, high, iterations):
            """Drive objective(x) to zero exactly the way the SQL macro does.

            Each round keeps the half of the bracket where the sign flips. The macro
            runs a fixed number of rounds rather than testing for convergence, so this
            does too, which is what lets both sides agree digit for digit.
            """
            if low is None or high is None:
                return None
            count = int(iterations) if iterations and int(iterations) > 0 else 60
            f_low = objective(low)
            if f_low is None:
                return None
            for _ in range(count):
                mid = (low + high) / 2.0
                f_mid = objective(mid)
                if f_mid is None:
                    return None
                if sign(f_mid) == sign(f_low):
                    low, f_low = mid, f_mid
                else:
                    high = mid
            return (low + high) / 2.0

        def irr_value(flows, low, high, iterations):
            if not flows or any(v is None for v in flows):
                return None

            def npv(candidate):
                try:
                    return sum(v / math.pow(1.0 + candidate, i) for i, v in enumerate(flows))
                except (ValueError, ZeroDivisionError, OverflowError):
                    return None

            return bisect(npv, low, high, iterations)

        def xirr_value(flows, offsets, low, high, iterations):
            if not flows or any(v is None for v in flows):
                return None
            if not offsets or any(d is None for d in offsets):
                return None

            def xnpv(candidate):
                try:
                    return sum(v / math.pow(1.0 + candidate, offsets[i] / 365.0)
                               for i, v in enumerate(flows))
                except (ValueError, ZeroDivisionError, OverflowError):
                    return None

            return bisect(xnpv, low, high, iterations)

        def rate_value(periods, payment, present, future, due, low, high, iterations):
            if None in (periods, payment, present, future, due):
                return None

            def residual(candidate):
                try:
                    if candidate == 0:
                        return present + payment * periods + future
                    growth = math.pow(1.0 + candidate, periods)
                    return (present * growth
                            + payment * (1 + candidate * due) * (growth - 1) / candidate
                            + future)
                except (ValueError, ZeroDivisionError, OverflowError):
                    return None

            return bisect(residual, low, high, iterations)

        values = [F.expr(str(c).strip()).cast("double")
                  for c in (props.valueColumns or []) if str(c).strip()]
        date_cols = [F.to_date(F.expr(str(c).strip()))
                     for c in (props.dateColumns or []) if str(c).strip()]

        def gap(i):
            """Whole days from the first date column to the i-th one."""
            return F.datediff(date_cols[i], date_cols[0]).cast("double")

        rate = num(props.rateCol)
        nper = num(props.nperCol)
        pmt = num(props.pmtCol)
        pv = num(props.pvCol)
        fv = num(props.fvCol)
        pay_type = num(props.paymentType)
        lo = num(props.loBoundCol, "-0.99")
        hi = num(props.hiBoundCol, "10")
        rounds = num(props.nIterCol, "60").cast("int")

        if fn == "cagr":
            result = F.pow(num(props.endValueCol) / nz(num(props.beginValueCol)),
                           F.lit(1.0) / num(props.periodsCol, "1")) - 1

        elif fn == "effectiverate":
            npery = num(props.nperyCol, "1")
            result = F.pow(1 + num(props.nominalRateCol) / nz(npery), npery) - 1

        elif fn == "nominalrate":
            npery = num(props.nperyCol, "1")
            result = npery * (F.pow(1 + num(props.effectRateCol), F.lit(1.0) / nz(npery)) - 1)

        elif fn == "fv":
            growth = F.pow(1 + rate, nper)
            result = F.when(rate == 0, -(pv + pmt * nper)).otherwise(
                -(pv * growth + pmt * (1 + rate * pay_type) * (growth - 1) / rate))

        elif fn == "pv":
            growth = F.pow(1 + rate, nper)
            result = F.when(rate == 0, -(fv + pmt * nper)).otherwise(
                -(fv + pmt * (1 + rate * pay_type) * (growth - 1) / rate) / growth)

        elif fn == "pmt":
            growth = F.pow(1 + rate, nper)
            result = F.when(rate == 0, -(pv + fv) / nz(nper)).otherwise(
                -(pv * growth + fv) / ((1 + rate * pay_type) * (growth - 1) / rate))

        elif fn == "nper":
            due = pmt * (1 + rate * pay_type)
            result = F.when(rate == 0, -(pv + fv) / nz(pmt)).otherwise(
                F.log((due - fv * rate) / nz(due + pv * rate)) / F.log(1 + rate))

        elif fn == "npv":
            result = total([v / F.pow(1 + rate, F.lit(float(i + 1)))
                            for i, v in enumerate(values)])

        elif fn == "xnpv":
            result = total([v / F.pow(1 + rate, gap(i) / 365.0)
                            for i, v in enumerate(values)])

        elif fn == "fvschedule":
            result = num(props.principalCol)
            for schedule_rate in values:
                result = result * (1 + schedule_rate)

        elif fn == "mirr":
            finance_rate = num(props.financeRateCol)
            reinvest_rate = num(props.reinvestRateCol)
            last = len(values) - 1
            gains = total([F.when(v > 0, v * F.pow(1 + reinvest_rate, F.lit(float(last - i))))
                           .otherwise(F.lit(0.0)) for i, v in enumerate(values)])
            costs = total([F.when(v < 0, v / F.pow(1 + finance_rate, F.lit(float(i))))
                           .otherwise(F.lit(0.0)) for i, v in enumerate(values)])
            result = F.pow(-gains / nz(costs), F.lit(1.0 / last)) - 1

        elif fn == "mxirr":
            finance_rate = num(props.financeRateCol)
            reinvest_rate = num(props.reinvestRateCol)
            span = gap(len(values) - 1)
            gains = total([F.when(v > 0, v * F.pow(1 + reinvest_rate, (span - gap(i)) / 365.0))
                           .otherwise(F.lit(0.0)) for i, v in enumerate(values)])
            costs = total([F.when(v < 0, v / F.pow(1 + finance_rate, gap(i) / 365.0))
                           .otherwise(F.lit(0.0)) for i, v in enumerate(values)])
            result = F.pow(-gains / nz(costs), F.lit(365.0) / nz(span)) - 1

        elif fn == "irr":
            result = F.udf(irr_value, DoubleType())(F.array(*values), lo, hi, rounds)

        elif fn == "xirr":
            offsets = F.array(*[gap(i) for i in range(len(values))])
            result = F.udf(xirr_value, DoubleType())(F.array(*values), offsets, lo, hi, rounds)

        elif fn == "rate":
            result = F.udf(rate_value, DoubleType())(nper, pmt, pv, fv, pay_type, lo, hi, rounds)

        else:
            result = F.lit(None).cast("double")

        return in0.withColumn(out_col, result.cast("double"))
