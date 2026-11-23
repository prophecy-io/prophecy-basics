import ast
import dataclasses
import json
import re
from abc import ABC

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class ValidationRule(ABC):
    pass


@dataclass(frozen=True)
class SortColumnExpr:
    expression: str
    format: Optional[str]


@dataclass(frozen=True)
class SortColumn:
    expression: SortColumnExpr
    sortType: str = "desc"
    _row_id: Optional[str] = None


NUMBER_RE = re.compile(r"^-?[0-9]+([.][0-9]+)?$")
TEXT_TYPES = {"string", "varchar", "char", "text"}
NUMBER_TYPES = {"integer", "int", "int64", "bigint", "smallint", "tinyint", "byte", "short", "long",
                "double", "float", "float64", "real", "decimal", "numeric", "bignumeric", "number"}

# Fields each check type uses; apply() emits only these (plus a non-empty checkName and reasonCode,
# and uniqueMode/orderColumns for a unique check that keeps one row).
RULE_FIELDS = {
    "not_null": ["columns"],
    "unique": ["columns"],
    "accepted_values": ["columns", "values"],
    "disallowed_values": ["columns", "values"],
    "value_range": ["columns", "minValue", "maxValue"],
    "regex_match": ["columns", "pattern"],
    "expression": ["expression"],
    "required_columns": ["columns"],
    "row_count": ["minValue", "maxValue"],
}

# UI only: (checkType, label, icon, scope, description) for each check type, in picker order
CHECK_TYPES = [
    ("not_null", "Not null", "CheckCircleIcon", "Per column", "A row fails when any selected column is null"),
    ("unique", "Unique", "HashAIcon", "Per column", "Rows fail when the selected columns repeat"),
    ("accepted_values", "Accepted values", "CheckDoneAIcon", "Per column", "Only the values on your list may appear"),
    ("disallowed_values", "Disallowed values", "SlashCircleAIcon", "Per column", "A row fails when a column holds a listed value"),
    ("value_range", "Value range", "SwitchHorizontalAIcon", "Per column", "Values must fall between a minimum and a maximum"),
    ("regex_match", "Pattern match", "AsteriskAIcon", "Per column", "The whole value must match a regular expression"),
    ("expression", "Custom SQL condition", "CodeAIcon", "Per row", "Rows where the condition is false fail"),
    ("required_columns", "Required columns", "ColumnsAIcon", "Whole input", "The input must contain the selected columns"),
    ("row_count", "Row count", "RowsAIcon", "Whole input", "The number of input rows must stay within bounds"),
]
COLUMN_CHECK_TYPES = ["not_null", "unique", "accepted_values", "disallowed_values", "value_range", "regex_match"]
ROW_AND_INPUT_CHECK_TYPES = ["expression", "required_columns", "row_count"]

MUTED_TONE = "#667085"
ACCENT_TONE = "#6938EF"


@dataclass
class StyledText(Text):
    """Text with a colour and a weight; both are display only."""
    tone: Optional[str] = None
    weight: Optional[int] = None

    def jsonProperties(self) -> dict:
        properties = super(StyledText, self).jsonProperties()
        if self.tone is not None:
            properties["tone"] = self.tone
        if self.weight is not None:
            properties["weight"] = self.weight
        return properties


@dataclass
class FullHeightAlertBox(AlertBox):
    """AlertBox that fills its column, so alerts side by side share one height; display only."""
    height: Optional[str] = "100%"

    def jsonProperties(self) -> dict:
        properties = super(FullHeightAlertBox, self).jsonProperties()
        if self.height is not None:
            properties["height"] = self.height
        return properties


class DataQuality(MacroSpec):
    name: str = "DataQuality"
    projectName: str = "prophecy_basics"
    category: str = "Custom"
    minNumOfInputPorts: int = 1
    # out0 = passed rows, out1 = failed rows, out2 = errors (one row per failed check)
    defaultOutputPorts: list[str] = ["out0", "out1", "out2"]
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        ProviderTypeEnum.Snowflake,
        ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]
    dependsOnUpstreamSchema: bool = True

    # macro output_mode returned on each output port, in port order
    portOutputModes: list[str] = ["passed", "failed", "errors"]

    @dataclass(frozen=True)
    class AddValidationRule(ValidationRule):
        checkType: str = "not_null"
        columns: List[str] = field(default_factory=list)
        values: str = ""
        pattern: str = ""
        expression: str = ""
        minValue: str = ""
        maxValue: str = ""
        uniqueMode: str = "fail_all"
        sortColumns: List[SortColumn] = field(default_factory=list)
        checkName: str = ""
        reasonCode: str = ""
        _row_id: Optional[str] = None

    @dataclass(frozen=True)
    class DataQualityProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        rules: List[ValidationRule] = field(default_factory=list)
        output_mode: str = "passed"
        on_failure: str = "continue"
        max_failed_rows: str = ""
        add_failed_codes: bool = False

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

    @staticmethod
    def _is_empty_schema(schema) -> bool:
        return schema is None or (isinstance(schema, str) and not schema.strip())

    def _add_rule(self, state: Component[DataQualityProperties], check_type: str = "not_null"):
        rules = list(state.properties.rules)
        rules.append(self.AddValidationRule(checkType=check_type))
        return state.bindProperties(dataclasses.replace(state.properties, rules=rules))

    def onButtonClick(self, state: Component[DataQualityProperties]):
        return self._add_rule(state)

    def _add_rule_handler(self, check_type: str):
        return lambda state: self._add_rule(state, check_type)

    def dialog(self) -> Dialog:
        RULE = "record.AddValidationRule"
        labels = {t: label for t, label, _, _, _ in CHECK_TYPES}
        icons = {t: icon for t, _, icon, _, _ in CHECK_TYPES}
        scopes = {t: scope for t, _, _, scope, _ in CHECK_TYPES}
        descriptions = {t: description for t, _, _, _, description in CHECK_TYPES}

        # ---------- small building blocks (each call returns new elements; containers share child lists) ----------
        def text(value, level=FontLevel.sm, tone=None, weight=None, italic=None):
            return StyledText(_children=[NativeText(value)], level=level, tone=tone, weight=weight, italic=italic)

        def caption(value, italic=None):
            return text(value, level=FontLevel.sm13, tone=MUTED_TONE, italic=italic)

        def bound_text(prop, **kwargs):
            return text(f"${{{prop}}}", **kwargs)

        def icon(name, color=ACCENT_TONE):
            return ProphecyIcon(type="default", iconName=name, color=color)

        def inline(*elements, gap="0.5rem"):
            stack = StackLayout(direction="horizontal", gap=gap, alignY="center")
            for element in elements:
                stack = stack.addElement(element)
            return stack

        def stack(*elements, gap="0.5rem"):
            result = StackLayout(gap=gap)
            for element in elements:
                result = result.addElement(element)
            return result

        def columns(*columns_and_widths, gap="1rem", align_y=None):
            layout = ColumnsLayout(gap, alignY=align_y)
            for element, width in columns_and_widths:
                layout = layout.addColumn(element, width)
            return layout

        def when_equal(prop, value, element, otherwise=None):
            condition = Condition().ifEqual(PropExpr(prop), StringExpr(value)).then(element)
            return condition.otherwise(otherwise) if otherwise is not None else condition

        def by_type(build):
            """One element per check type, as a single if / else-if chain on the check type."""
            element = None
            for check_type, *_ in reversed(CHECK_TYPES):
                element = when_equal(f"{RULE}.checkType", check_type, build(check_type), element)
            return element

        # ---------- check settings ----------
        def column_picker(title="Columns"):
            return (
                SchemaColumnsDropdown(title)
                .withMultipleSelection()
                .bindSchema("component.ports.inputs[0].schema")
                .bindProperty(f"{RULE}.columns")
            )

        def min_max(min_title, max_title):
            return stack(
                columns(
                    (columns(
                        (TextBox(min_title).bindPlaceholder("No minimum").bindProperty(f"{RULE}.minValue"), "1fr"),
                        (caption("to"), "content"),
                        (TextBox(max_title).bindPlaceholder("No maximum").bindProperty(f"{RULE}.maxValue"), "1fr"),
                        gap="0.75rem", align_y="end",
                    ), "2fr"),
                    (None, "1fr"),
                ),
            )

        sort_order = stack(
            text("Which duplicate row to keep", weight=600),
            BasicTable(
                "Sort columns",
                height="200px",
                columns=[
                    Column(
                        "Column or expression",
                        "expression.expression",
                        ExpressionBox(ignoreTitle=True, language="sql")
                        .bindPlaceholders()
                        .withSchemaSuggestions()
                        .bindLanguage("${record.expression.format}"),
                    ),
                    Column(
                        "Keep the row with the",
                        "sortType",
                        SelectBox("")
                        .addOption("Highest / latest value", "desc")
                        .addOption("Lowest / earliest value", "asc"),
                        width="35%",
                    ),
                ],
            ).bindProperty(f"{RULE}.sortColumns"),
        )

        def values_settings(title, placeholder, hint):
            return columns(
                (column_picker(), "1fr"),
                (stack(
                    TextBox(title).bindPlaceholder(placeholder).bindProperty(f"{RULE}.values"),
                    caption(hint),
                ), "2fr"),
            )

        def settings(check_type):
            if check_type == "not_null":
                return stack(
                    column_picker(),
                    AlertBox(variant="info", _children=[text("No other settings. Pick as many columns as you need.")]),
                    gap="0.75rem",
                )
            if check_type == "unique":
                return columns(
                    (stack(
                        column_picker(),
                        RadioGroup("Duplicates", orientation="horizontal")
                        .addOption("Fail every copy", "fail_all")
                        .addOption("Keep one row, fail the rest", "keep_first")
                        .setOptionType("button")
                        .setVariant("medium")
                        .setButtonStyle("solid")
                        .bindProperty(f"{RULE}.uniqueMode"),
                        when_equal(
                            f"{RULE}.uniqueMode", "keep_first",
                            caption("One row from each group of duplicates passes; the others fail."),
                            caption("Every row in a group of duplicates fails."),
                        ),
                        gap="0.75rem",
                    ), "1fr"),
                    (when_equal(f"{RULE}.uniqueMode", "keep_first", sort_order), "1fr"),
                )
            if check_type == "accepted_values":
                return values_settings(
                    "Allowed values", "open, shipped, cancelled",
                    "Comma-separated. Rows whose value isn't in this list fail.",
                )
            if check_type == "disallowed_values":
                return values_settings(
                    "Values that aren't allowed", "unknown, test",
                    "Comma-separated. Rows holding any of these values fail.",
                )
            if check_type == "value_range":
                return stack(
                    column_picker(),
                    min_max("Minimum", "Maximum"),
                    caption("Leave either end empty for an open range."),
                    gap="0.75rem",
                )
            if check_type == "regex_match":
                return columns(
                    (column_picker(), "1fr"),
                    (stack(
                        TextBox("Pattern").bindPlaceholder("[A-Z]{2}-[0-9]{4}").bindProperty(f"{RULE}.pattern"),
                        caption("The whole value must match: [A-Z]{2}-[0-9]{4} matches AB-1234 but not AB-12."),
                    ), "2fr"),
                )
            if check_type == "expression":
                return stack(
                    ExpressionBox(title="SQL condition", language="sql")
                    .bindPlaceholder("ship_date >= order_date")
                    .withSchemaSuggestions()
                    .bindProperty(f"{RULE}.expression"),
                    caption("Write any condition every row must meet. Rows where it's false fail."),
                )
            if check_type == "required_columns":
                return stack(
                    column_picker(),
                    caption("Lists the columns the input must contain."),
                )
            return stack(
                min_max("Minimum rows", "Maximum rows"),
                caption("No column picker: this check looks at the whole input."),
            )

        check_type_picker = SelectBox("Check type")
        for check_type, label, *_ in CHECK_TYPES:
            check_type_picker = check_type_picker.addOption(label, check_type)
        check_type_picker = check_type_picker.bindProperty(f"{RULE}.checkType")

        card_body = stack(
            HorizontalDivider(),
            columns(
                (check_type_picker, "1fr"),
                (TextBox("Check name · optional").bindPlaceholder("Shown in the results")
                 .bindProperty(f"{RULE}.checkName"), "1fr"),
                (TextBox("Reason code · optional").bindPlaceholder("Defaults to the check type, e.g. NOT_NULL")
                 .bindProperty(f"{RULE}.reasonCode"), "1fr"),
            ),
            by_type(settings),
            gap="1rem",
        )

        # Header: icon, name (or type and "Unnamed"), description, scope and reason code
        card_title = when_equal(
            f"{RULE}.checkName", "",
            inline(
                by_type(lambda t: text(labels[t], weight=600)),
                caption("Unnamed", italic=True),
            ),
            inline(
                bound_text(f"{RULE}.checkName", weight=600),
                by_type(lambda t: text(labels[t], level=FontLevel.sm13, tone=ACCENT_TONE, weight=500)),
            ),
        )
        reason_badge = when_equal(
            f"{RULE}.reasonCode", "",
            caption("Default reason code"),
            inline(caption("Reason"), bound_text(f"{RULE}.reasonCode", level=FontLevel.sm13, weight=600), gap="0.25rem"),
        )
        card_header = columns(
            (by_type(lambda t: icon(icons[t])), "content"),
            (stack(card_title, by_type(lambda t: caption(descriptions[t]))), "1fr"),
            (by_type(lambda t: caption(scopes[t])), "content"),
            (reason_badge, "content"),
            gap="0.75rem", align_y="center",
        )

        # The card's header carries the row's delete button; the list lets checks be dragged to reorder
        check_list = (
            OrderedList("Checks")
            .bindProperty("rules")
            .setEmptyContainerText("No checks yet. Add one below to start validating your data.")
            .addElement(Card(header=card_header, collapsible=True).addElement(card_body))
        )

        def button(label, on_click, icon_name, variant=ButtonVariant.secondaryGrey, block=None):
            return (
                Button(label, variant=variant, block=block)
                .bindOnClick(on_click)
                .addElement(inline(icon(icon_name, color="currentColor"), NativeText(label)))
            )

        def add_buttons(check_types):
            # Three buttons per row, like the check type picker
            rows = []
            for start in range(0, len(check_types), 3):
                row_types = check_types[start:start + 3]
                cells = [(button(labels[t], self._add_rule_handler(t), icons[t], block=True), "1fr") for t in row_types]
                cells += [(None, "1fr")] * (3 - len(row_types))
                rows.append(columns(*cells, gap="0.75rem"))
            return stack(*rows)

        add_check_panel = stack(
            HorizontalDivider(),
            inline(
                icon("PlusIcon"),
                stack(text("Add a check", weight=600), caption("Start from a check type. You can change it later."),
                      gap="0.125rem"),
                gap="0.75rem",
            ),
            text("COLUMN CHECKS", level=FontLevel.xs, tone=MUTED_TONE, weight=600),
            add_buttons(COLUMN_CHECK_TYPES),
            text("ROW AND INPUT CHECKS", level=FontLevel.xs, tone=MUTED_TONE, weight=600),
            add_buttons(ROW_AND_INPUT_CHECK_TYPES),
            gap="0.75rem",
        )

        checks = stack(
            columns(
                (stack(TitleElement("Checks"),
                       caption("Every input row runs through these checks. Drag a check to reorder it.")), "1fr"),
                (button("Add check", self.onButtonClick, "PlusIcon", variant=ButtonVariant.primary), "content"),
                align_y="center",
            ),
            check_list,
            add_check_panel,
            gap="1rem",
        )

        # ---------- outputs ----------
        def port_tile(variant, port, name, description):
            return FullHeightAlertBox(
                variant=variant,
                _children=[stack(
                    inline(text(port, level=FontLevel.sm13, weight=700), text(name, weight=600)),
                    text(description, level=FontLevel.sm13),
                    gap="0.25rem",
                )],
            )

        outputs = stack(
            columns(
                (TitleElement("Outputs"), "1fr"),
                (caption("in0 · every input row runs through every check"), "content"),
                align_y="center",
            ),
            columns(
                # Same-length descriptions, and stretched columns, keep the three tiles one height
                (port_tile("success", "out0", "Passed",
                           "Rows that pass every check, with the input columns only."), "1fr"),
                (port_tile("warning", "out1", "Failed",
                           "Rows that fail any check, with dq_failed_checks added."), "1fr"),
                (port_tile("error", "out2", "Errors",
                           "One row per failed check: failing row count, message."), "1fr"),
                align_y="stretch",
            ),
            HorizontalDivider(),
            stack(
                Switch("Add dq_failed_codes to failed rows").bindProperty("add_failed_codes"),
                caption("The reason codes of the checks each row failed."),
                gap="0.25rem",
            ),
            gap="1rem",
        )

        # ---------- failure handling ----------
        on_failure_note = when_equal(
            "component.properties.on_failure", "fail",
            caption("The pipeline stops with an error."),
            when_equal(
                "component.properties.on_failure", "block",
                caption("Failed rows and errors still flow; out0 gets no rows."),
                caption("Passed rows keep flowing through out0."),
            ),
        )
        on_failure = stack(
            columns(
                (stack(TitleElement("When any check fails"), on_failure_note, gap="0.25rem"), "1fr"),
                (SelectBox("")
                 .addOption("Continue with the passed rows", "continue")
                 .addOption("Continue, but send no rows to Passed", "block")
                 .addOption("Stop the pipeline with an error", "fail")
                 .bindProperty("on_failure"), "1fr"),
                align_y="center",
            ),
            Condition()
            .ifNotEqual(PropExpr("component.properties.on_failure"), StringExpr("continue"))
            .then(columns(
                (None, "1fr"),
                (stack(
                    TextBox("Failing rows allowed · optional").bindPlaceholder("0").bindProperty("max_failed_rows"),
                    caption("Applies only when more rows fail than this. Leave empty to allow none."),
                    gap="0.25rem",
                ), "1fr"),
            )),
            gap="1rem",
        )

        def panel(element):
            return StepContainer().addElement(Step().addElement(element))

        return Dialog("DataQuality").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(allowOutputRename=True), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(panel(checks))
                .addElement(panel(outputs))
                .addElement(panel(on_failure))
            )
        )

    @staticmethod
    def _rule_value(rule, key, default=""):
        if isinstance(rule, dict):
            return rule.get(key, default)
        return getattr(rule, key, default)

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(DataQuality, self).validate(context, component)
        props = component.properties

        def error(path, message):
            diagnostics.append(Diagnostic(path, message, SeverityLevelEnum.Error))

        if not props.rules:
            error("component.properties.rules", "Add at least one check")
            return diagnostics

        schema_columns = []
        column_types = {}
        try:
            for f in json.loads(props.schema or "[]"):
                column_name = f["name"].lower()
                schema_columns.append(column_name)
                column_types[column_name] = str(f.get("dataType", "")).split("(")[0].strip().lower()
        except (ValueError, TypeError, KeyError, AttributeError):
            schema_columns = []
            column_types = {}

        for i, rule in enumerate(props.rules):
            path = f"component.properties.rules[{i}]"
            label = f"Check {i + 1}"
            check_type = self._rule_value(rule, "checkType")
            fields = RULE_FIELDS.get(check_type)
            if fields is None:
                error(path, f"{label}: choose a check type")
                continue
            columns = list(self._rule_value(rule, "columns", []) or [])
            min_value = (self._rule_value(rule, "minValue") or "").strip()
            max_value = (self._rule_value(rule, "maxValue") or "").strip()

            if "columns" in fields:
                if not columns:
                    error(path, f"{label}: select at least one column")
                elif schema_columns and check_type != "required_columns":
                    missing = [c for c in columns if c.lower() not in schema_columns]
                    if missing:
                        error(path, f"{label}: column(s) {missing} are not in the input")
            if check_type == "required_columns" and not schema_columns:
                error(path, f"{label}: the input schema is not available yet")
            if "values" in fields and not [v for v in (self._rule_value(rule, "values") or "").split(",") if v.strip()]:
                error(path, f"{label}: enter at least one value")
            if "pattern" in fields and not (self._rule_value(rule, "pattern") or "").strip():
                error(path, f"{label}: enter a pattern")
            if "expression" in fields and not (self._rule_value(rule, "expression") or "").strip():
                error(path, f"{label}: enter a SQL condition")
            if "minValue" in fields and not min_value and not max_value:
                error(path, f"{label}: enter a minimum, a maximum, or both")
            if check_type == "row_count":
                for bound in (min_value, max_value):
                    if bound and not bound.isdigit():
                        error(path, f"{label}: minimum and maximum rows must be 0 or a positive whole number (leave one empty for no limit), got '{bound}'")
                if min_value.isdigit() and max_value.isdigit() and int(min_value) > int(max_value):
                    error(path, f"{label}: the minimum is larger than the maximum")
            if check_type == "value_range":
                for col in columns:
                    col_type = column_types.get(col.lower(), "")
                    for bound in [min_value, max_value]:
                        if not bound:
                            continue
                        if col_type in TEXT_TYPES and NUMBER_RE.match(bound):
                            error(path, f"{label}: {col} is a text column and cannot be compared with the number {bound}. Convert the column to a number before this gem, or use text bounds.")
                        elif col_type in NUMBER_TYPES and not NUMBER_RE.match(bound):
                            error(path, f"{label}: {col} is a number column and cannot be compared with the text '{bound}'. Use a number as the bound.")
            if check_type == "unique" and self._rule_value(rule, "uniqueMode") == "keep_first":
                sort_columns = list(self._rule_value(rule, "sortColumns", []) or [])
                if not self._sort_columns_as_dicts(sort_columns):
                    error(path, f"{label}: add at least one column that decides which duplicate row is kept")
                for j, sort_column in enumerate(sort_columns):
                    column = self._sort_column_name(sort_column)
                    if column and schema_columns and column.lower() not in schema_columns:
                        error(f"{path}.sortColumns[{j}].expression.expression",
                              f"{label}, sort column {j + 1}: {column} is not a column of the input. Enter a column name.")

        if props.output_mode not in ("passed", "failed", "all", "summary", "errors"):
            error("component.properties.output_mode", "Choose an output")
        if props.on_failure not in ("continue", "block", "fail"):
            error("component.properties.on_failure", "Choose what happens when a check fails")
        max_failed_rows = (props.max_failed_rows or "").strip()
        if props.on_failure != "continue" and max_failed_rows and not max_failed_rows.isdigit():
            error("component.properties.max_failed_rows",
                  f"Failing rows allowed must be 0 or a positive whole number (leave it empty to allow none), got '{max_failed_rows}'")
        if len(component.ports.outputs) > len(self.portOutputModes):
            error("component.ports.outputs",
                  f"DataQuality has at most {len(self.portOutputModes)} outputs: passed rows, failed rows and errors")
        return diagnostics

    def _schema_and_relations(self, component: Component, context: SqlContext):
        if self._is_empty_schema(component.ports.inputs[0].schema):
            raise ValueError(f"{self.name}: input schema is empty")
        schema = (json.loads(component.ports.inputs[0].schema) if isinstance(component.ports.inputs[0].schema, str) else (component.ports.inputs[0].schema or {}))
        fields_array = [
            {"name": field["name"], "dataType": field["dataType"]["type"]}
            for field in schema["fields"]
        ]
        return dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=self.get_relation_names(component, context),
        )

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        return self._with_port_hints(newState.bindProperties(self._schema_and_relations(newState, context)))

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        return self._with_port_hints(component.bindProperties(self._schema_and_relations(component, context)))

    def _with_port_hints(self, component: Component) -> Component:
        # Tell the UI what each output port holds, its icon and the input it comes from
        # Hints are display only: on a CBP without these fields or icons, keep the component as it is
        try:
            props = component.properties
            passed = "Rows that pass every check"
            if props.on_failure == "block":
                passed += " (empty when checks fail)"
            hints = [
                (passed, PortIconEnum.FilterLines),
                ("Rows that fail at least one check", PortIconEnum.FilterLines),
                ("One row per failed check", PortIconEnum.Sigma),
            ]
            outputs = list(component.ports.outputs)
            for idx, (description, icon) in enumerate(hints):
                if idx >= len(outputs):
                    break
                outputs[idx] = dataclasses.replace(
                    outputs[idx],
                    description=description,
                    icon=icon,
                    incomingAliases=list(props.relation_name or []),
                )
            return component.bindPorts(dataclasses.replace(component.ports, outputs=outputs))
        except (AttributeError, NameError, TypeError):
            return component

    def _sort_column_name(self, sort_column) -> str:
        """The column typed in a sort row, without the quotes that schema suggestions may add."""
        expression = self._rule_value(sort_column, "expression")
        name = (self._rule_value(expression, "expression") if expression else "") or ""
        name = name.strip()
        if len(name) >= 2 and name[0] == name[-1] and name[0] in (chr(96), chr(34)):
            name = name[1:-1].strip()
        return name

    def _sort_columns_as_dicts(self, sort_columns) -> list:
        """Rows left empty are skipped."""
        result = []
        for sort_column in sort_columns or []:
            name = self._sort_column_name(sort_column)
            if name:
                result.append({
                    "column": name,
                    "direction": self._rule_value(sort_column, "sortType") or "desc",
                })
        return result

    def _rules_as_dicts(self, rules) -> list:
        result = []
        for rule in rules:
            check_type = self._rule_value(rule, "checkType")
            entry = {"checkType": check_type}
            for key in RULE_FIELDS.get(check_type, []):
                value = self._rule_value(rule, key, [] if key == "columns" else "")
                if key == "columns":
                    value = list(value or [])
                if value:
                    entry[key] = value
            if check_type == "unique" and self._rule_value(rule, "uniqueMode") == "keep_first":
                entry["uniqueMode"] = "keep_first"
                entry["orderColumns"] = self._sort_columns_as_dicts(self._rule_value(rule, "sortColumns", []))
            for key in ("checkName", "reasonCode"):
                if self._rule_value(rule, key):
                    entry[key] = self._rule_value(rule, key)
            result.append(entry)
        return result

    @staticmethod
    def _failure_settings(props) -> tuple:
        """max_failed_rows only applies when blocking or stopping on failure."""
        max_failed_rows = (props.max_failed_rows or "").strip() if props.on_failure in ("block", "fail") else ""
        return max_failed_rows, bool(props.add_failed_codes)

    def _macro_call(self, props: DataQualityProperties, output_mode: str) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"
        max_failed_rows, add_failed_codes = self._failure_settings(props)
        # repr() emits Python literals that Jinja parses as-is and loadProperties reads back with ast.literal_eval
        arguments = [
            repr(list(props.relation_name)),
            repr(props.schema or ""),
            repr(self._rules_as_dicts(props.rules)),
            repr(output_mode),
            repr(props.on_failure),
            repr(max_failed_rows),
            str(add_failed_codes).lower(),
        ]
        return f"{{{{ {resolved_macro_name}({', '.join(arguments)}) }}}}"

    def apply(self, props: DataQualityProperties) -> str:
        # Used when the gem has a single output port
        return self._macro_call(props, props.output_mode)

    def applyMulti(self, props: DataQualityProperties, outputPorts: list[str]) -> list[str]:
        # One macro call per output port: out0 = passed rows, out1 = failed rows, out2 = errors
        if len(outputPorts) <= 1:
            return [self.apply(props)]
        return [self._macro_call(props, output_mode) for output_mode in self.portOutputModes[: len(outputPorts)]]

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)

        def _parse_py_literal(raw, default):
            raw = (raw or "").strip()
            if not raw:
                return default
            try:
                return ast.literal_eval(raw)
            except (ValueError, SyntaxError):
                return raw.strip("'\"") if isinstance(default, str) else default

        rules = []
        for rule in _parse_py_literal(parametersMap.get("rules"), []):
            if not isinstance(rule, dict):
                continue
            sort_columns = []
            for order_column in rule.get("orderColumns") or []:
                if isinstance(order_column, dict):
                    sort_columns.append(SortColumn(
                        expression=SortColumnExpr(expression=str(order_column.get("column") or ""), format="sql"),
                        sortType=str(order_column.get("direction") or "desc"),
                    ))
            columns = rule.get("columns") or []
            rules.append(DataQuality.AddValidationRule(
                checkType=str(rule.get("checkType", "not_null")),
                columns=[columns] if isinstance(columns, str) else [str(c) for c in columns],
                values=str(rule.get("values") or ""),
                pattern=str(rule.get("pattern") or ""),
                expression=str(rule.get("expression") or ""),
                minValue="" if rule.get("minValue") is None else str(rule.get("minValue")),
                maxValue="" if rule.get("maxValue") is None else str(rule.get("maxValue")),
                uniqueMode=str(rule.get("uniqueMode") or "fail_all"),
                sortColumns=sort_columns,
                checkName=str(rule.get("checkName") or ""),
                reasonCode=str(rule.get("reasonCode") or ""),
            ))

        max_failed_rows = _parse_py_literal(parametersMap.get("max_failed_rows"), "")
        return DataQuality.DataQualityProperties(
            relation_name=_parse_py_literal(parametersMap.get("relation_name"), []),
            schema=_parse_py_literal(parametersMap.get("schema"), ""),
            rules=rules,
            output_mode=_parse_py_literal(parametersMap.get("output_mode"), "passed") or "passed",
            on_failure=_parse_py_literal(parametersMap.get("on_failure"), "continue") or "continue",
            max_failed_rows="" if max_failed_rows is None else str(max_failed_rows),
            add_failed_codes=(parametersMap.get("add_failed_codes") or "").strip().lower() == "true",
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        max_failed_rows, add_failed_codes = self._failure_settings(properties)
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", repr(list(properties.relation_name))),
                MacroParameter("schema", repr(properties.schema or "")),
                MacroParameter("rules", repr(self._rules_as_dicts(properties.rules))),
                MacroParameter("output_mode", repr(properties.output_mode)),
                MacroParameter("on_failure", repr(properties.on_failure)),
                MacroParameter("max_failed_rows", repr(max_failed_rows)),
                MacroParameter("add_failed_codes", str(add_failed_codes).lower()),
            ],
        )
