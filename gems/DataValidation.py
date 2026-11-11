import ast
import dataclasses
import json
import re
from abc import ABC

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class ValidationRule(ABC):
    pass


# Fields each check type uses; apply() emits only these (plus a non-empty checkName).
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


class DataValidation(MacroSpec):
    name: str = "DataValidation"
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
    class AddValidationRule(ValidationRule):
        checkType: str = "not_null"
        columns: List[str] = field(default_factory=list)
        values: str = ""
        pattern: str = ""
        expression: str = ""
        minValue: str = ""
        maxValue: str = ""
        checkName: str = ""
        _row_id: Optional[str] = None

    @dataclass(frozen=True)
    class DataValidationProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        rules: List[ValidationRule] = field(default_factory=list)
        output_mode: str = "passed"
        on_failure: str = "continue"

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

    def onButtonClick(self, state: Component[DataValidationProperties]):
        rules = list(state.properties.rules)
        rules.append(self.AddValidationRule())
        return state.bindProperties(dataclasses.replace(state.properties, rules=rules))

    def dialog(self) -> Dialog:
        def when_type(check_type, element):
            return (
                Condition()
                .ifEqual(PropExpr("record.AddValidationRule.checkType"), StringExpr(check_type))
                .then(element)
            )

        def unless_type(check_types, element):
            condition = element
            for check_type in check_types:
                condition = (
                    Condition()
                    .ifNotEqual(PropExpr("record.AddValidationRule.checkType"), StringExpr(check_type))
                    .then(condition)
                )
            return condition

        check_type = (
            SelectBox("Check")
            .addOption("Not null", "not_null")
            .addOption("Unique", "unique")
            .addOption("Accepted values", "accepted_values")
            .addOption("Disallowed values", "disallowed_values")
            .addOption("Value range", "value_range")
            .addOption("Pattern match (regex)", "regex_match")
            .addOption("Custom SQL condition", "expression")
            .addOption("Required columns", "required_columns")
            .addOption("Row count", "row_count")
            .bindProperty("record.AddValidationRule.checkType")
        )

        columns = unless_type(
            ["expression", "row_count"],
            SchemaColumnsDropdown("Columns")
            .withMultipleSelection()
            .bindSchema("component.ports.inputs[0].schema")
            .bindProperty("record.AddValidationRule.columns"),
        )

        def min_max(min_title, max_title):
            return (
                ColumnsLayout("1rem")
                .addColumn(
                    TextBox(min_title).bindPlaceholder("optional").bindProperty("record.AddValidationRule.minValue"),
                    "0.5fr",
                )
                .addColumn(
                    TextBox(max_title).bindPlaceholder("optional").bindProperty("record.AddValidationRule.maxValue"),
                    "0.5fr",
                )
            )

        rule_settings = (
            StackLayout(gap="0.5rem")
            .addElement(
                ColumnsLayout("1rem")
                .addColumn(check_type, "0.35fr")
                .addColumn(columns, "0.65fr")
            )
            .addElement(when_type(
                "accepted_values",
                TextBox("Allowed values (comma-separated)").bindPlaceholder("open, shipped, cancelled")
                .bindProperty("record.AddValidationRule.values"),
            ))
            .addElement(when_type(
                "disallowed_values",
                TextBox("Values that are not allowed (comma-separated)").bindPlaceholder("unknown, test")
                .bindProperty("record.AddValidationRule.values"),
            ))
            .addElement(when_type(
                "regex_match",
                TextBox("Pattern the whole value must match").bindPlaceholder("[A-Z]{2}-[0-9]{4}")
                .bindProperty("record.AddValidationRule.pattern"),
            ))
            .addElement(when_type(
                "expression",
                TextBox("SQL condition every row must meet").bindPlaceholder("ship_date >= order_date")
                .bindProperty("record.AddValidationRule.expression"),
            ))
            .addElement(when_type("value_range", min_max("Minimum value", "Maximum value")))
            .addElement(when_type("row_count", min_max("Minimum rows", "Maximum rows")))
            .addElement(
                TextBox("Check name (optional)").bindPlaceholder("Shown in the results")
                .bindProperty("record.AddValidationRule.checkName")
            )
        )

        checks = (
            StackLayout(gap="1rem", height="100%")
            .addElement(TitleElement("Checks"))
            .addElement(
                OrderedList("Checks")
                .bindProperty("rules")
                .setEmptyContainerText("Add a check to validate your data")
                .addElement(
                    ColumnsLayout("1rem", alignY="end")
                    .addColumn(rule_settings)
                    .addColumn(ListItemDelete("delete"), width="content")
                )
            )
            .addElement(SimpleButtonLayout("Add check", self.onButtonClick))
        )

        output_mode = (
            SelectBox("Output")
            .addOption("Rows that pass every check", "passed")
            .addOption("Rows that fail a check (adds dq_failed_checks)", "failed")
            .addOption("All rows with results (adds dq_passed, dq_failed_checks)", "all")
            .addOption("Summary per check (one row per check)", "summary")
            .bindProperty("output_mode")
        )

        on_failure = (
            SelectBox("When any check fails")
            .addOption("Continue", "continue")
            .addOption("Stop the pipeline with an error", "fail")
            .bindProperty("on_failure")
        )

        return Dialog("DataValidation").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(StepContainer().addElement(Step().addElement(checks)))
                .addElement(StepContainer().addElement(Step().addElement(output_mode)))
                .addElement(StepContainer().addElement(Step().addElement(on_failure)))
            )
        )

    @staticmethod
    def _rule_value(rule, key, default=""):
        if isinstance(rule, dict):
            return rule.get(key, default)
        return getattr(rule, key, default)

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(DataValidation, self).validate(context, component)
        props = component.properties

        def error(path, message):
            diagnostics.append(Diagnostic(path, message, SeverityLevelEnum.Error))

        if not props.rules:
            error("component.properties.rules", "Add at least one check")
            return diagnostics

        try:
            schema_columns = [f["name"].lower() for f in json.loads(props.schema or "[]")]
        except (ValueError, TypeError, KeyError):
            schema_columns = []

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

        if props.output_mode not in ("passed", "failed", "all", "summary"):
            error("component.properties.output_mode", "Choose an output")
        if props.on_failure not in ("continue", "fail"):
            error("component.properties.on_failure", "Choose what happens when a check fails")
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
        return newState.bindProperties(self._schema_and_relations(newState, context))

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        return component.bindProperties(self._schema_and_relations(component, context))

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
            if self._rule_value(rule, "checkName"):
                entry["checkName"] = self._rule_value(rule, "checkName")
            result.append(entry)
        return result

    def apply(self, props: DataValidationProperties) -> str:
        resolved_macro_name = f"{self.projectName}.{self.name}"
        # repr() emits Python literals that Jinja parses as-is and loadProperties reads back with ast.literal_eval
        arguments = [
            repr(list(props.relation_name)),
            repr(props.schema or ""),
            repr(self._rules_as_dicts(props.rules)),
            repr(props.output_mode),
            repr(props.on_failure),
        ]
        return f"{{{{ {resolved_macro_name}({', '.join(arguments)}) }}}}"

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
            columns = rule.get("columns") or []
            rules.append(DataValidation.AddValidationRule(
                checkType=str(rule.get("checkType", "not_null")),
                columns=[columns] if isinstance(columns, str) else [str(c) for c in columns],
                values=str(rule.get("values") or ""),
                pattern=str(rule.get("pattern") or ""),
                expression=str(rule.get("expression") or ""),
                minValue="" if rule.get("minValue") is None else str(rule.get("minValue")),
                maxValue="" if rule.get("maxValue") is None else str(rule.get("maxValue")),
                checkName=str(rule.get("checkName") or ""),
            ))

        return DataValidation.DataValidationProperties(
            relation_name=_parse_py_literal(parametersMap.get("relation_name"), []),
            schema=_parse_py_literal(parametersMap.get("schema"), ""),
            rules=rules,
            output_mode=_parse_py_literal(parametersMap.get("output_mode"), "passed") or "passed",
            on_failure=_parse_py_literal(parametersMap.get("on_failure"), "continue") or "continue",
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", repr(list(properties.relation_name))),
                MacroParameter("schema", repr(properties.schema or "")),
                MacroParameter("rules", repr(self._rules_as_dicts(properties.rules))),
                MacroParameter("output_mode", repr(properties.output_mode)),
                MacroParameter("on_failure", repr(properties.on_failure)),
            ],
        )
