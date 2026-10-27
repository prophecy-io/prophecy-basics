import dataclasses
import json
import re

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import ast
import xml.etree.ElementTree as ET

from jinja2 import Environment, TemplateSyntaxError, nodes

# Kinds of output column the Snowflake macro can extract (see macros/XMLParse.sql).
XML_OUTPUT_KINDS = ("root", "root_outer", "root_attr", "child", "child_outer", "child_attr")


def _jinja_str(value) -> str:
    """Single-quoted Jinja string literal; quotes and backslashes in the value are escaped."""
    text = "" if value is None else str(value)
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _decode_str(raw) -> str:
    """A string argument as loadProperties receives it: the call's source text when the gem is read
    back from code ('it\\'s' -> it's), or the plain saved value otherwise."""
    if raw is None:
        return ""
    text = str(raw)
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "'\"":
        try:
            parsed = Environment().parse("{{ " + text + " }}")
            node = parsed.body[0].nodes[0] if len(parsed.body) == 1 else None
            if isinstance(node, nodes.Const) and isinstance(node.value, str):
                return node.value
        except (TemplateSyntaxError, AttributeError, IndexError):
            pass
        return text[1:-1]
    return text


def _unique_name(name: str, used: set) -> str:
    """Alteryx renames a clashing field X to X2, X3, ..."""
    if name not in used:
        return name
    n = 2
    while f"{name}{n}" in used:
        n += 1
    return f"{name}{n}"


def output_columns_from_sample(sample_xml: str, input_columns: list, include_outer_xml: bool = False,
                               child_element: str = "") -> list:
    """Output columns Alteryx's XML Parse (return child values) creates for a sample document, in
    Alteryx's order, for the root element or, with child_element, for the first element with that
    tag below the root: the root value, the first occurrence of every child
    tag (each followed by its OuterXML), the root's OuterXML, then the repeated occurrences
    named <tag>_2, <tag>_3, ...  Names that clash with an input column get Alteryx's 2, 3, ...
    suffix.  Raises ValueError when the sample is not well-formed XML or has no child_element."""
    try:
        root = ET.fromstring(sample_xml)
    except ET.ParseError as e:
        raise ValueError(f"Sample record is not well-formed XML: {e}")
    if any(e.tag.startswith("{") for e in root.iter()):
        raise ValueError("Sample record uses XML namespaces; write the output columns by hand, "
                         "with tags as prefix:name")
    if child_element:
        root = next((e for e in root.iter(child_element) if e is not root), None)
        if root is None:
            raise ValueError(f"Sample record has no <{child_element}> element below the root")
    used = set(input_columns or [])
    columns = []

    def add(base, entry):
        name = _unique_name(base, used)
        used.add(name)
        columns.append(dict(entry, name=name))

    add(root.tag, {"kind": "root"})
    seen = {}
    repeats = []
    for child in root:
        instance = seen.get(child.tag, 0)
        seen[child.tag] = instance + 1
        if instance == 0:
            add(child.tag, {"kind": "child", "tag": child.tag, "instance": 0})
            if include_outer_xml:
                add(child.tag + "_OuterXML", {"kind": "child_outer", "tag": child.tag, "instance": 0})
        else:
            repeats.append((child.tag, instance))
    if include_outer_xml:
        add(root.tag + "_OuterXML", {"kind": "root_outer"})
    for tag, instance in repeats:
        base = f"{tag}_{instance + 1}"
        add(base, {"kind": "child", "tag": tag, "instance": instance})
        if include_outer_xml:
            add(base + "_OuterXML", {"kind": "child_outer", "tag": tag, "instance": instance})
    return columns


def _load_bool(raw, default: bool) -> bool:
    if raw is None or str(raw).strip("'\" ") == "":
        return default
    return str(raw).strip("'\" ").lower() == "true"


def parse_output_columns(raw) -> list:
    """outputColumns is stored as a JSON string; an empty value means none."""
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        return []
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as e:
            # A macro call parsed back from code may carry a Python-style literal ({'name': ...}).
            try:
                value = ast.literal_eval(raw)
            except (ValueError, SyntaxError):
                raise ValueError(str(e))
    else:
        value = raw
    if not isinstance(value, list):
        raise ValueError("outputColumns must be a JSON list")
    return value


def output_column_errors(columns: list) -> List[str]:
    errors = []
    names = set()
    for i, c in enumerate(columns):
        if not isinstance(c, dict):
            errors.append(f"Output column {i + 1} must be an object")
            continue
        name, kind = c.get("name"), c.get("kind")
        if not name:
            errors.append(f"Output column {i + 1} has no name")
        elif name in names:
            errors.append(f"Output column name '{name}' is used more than once")
        names.add(name)
        if kind not in XML_OUTPUT_KINDS:
            errors.append(f"Output column '{name}' has unknown kind '{kind}'; expected one of {', '.join(XML_OUTPUT_KINDS)}")
            continue
        if kind.startswith("child") and not c.get("tag"):
            errors.append(f"Output column '{name}' needs a tag")
        if kind.endswith("_attr") and not c.get("attr"):
            errors.append(f"Output column '{name}' needs an attr")
        instance = c.get("instance", 0)
        if not isinstance(instance, int) or isinstance(instance, bool) or instance < 0:
            errors.append(f"Output column '{name}' instance must be a non-negative integer")
    return errors


class XMLParse(MacroSpec):
    name: str = "XMLParse"
    projectName: str = "prophecy_basics"
    category: str = "Parse"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        ProviderTypeEnum.Snowflake,
        # ProviderTypeEnum.BigQuery,
        # ProviderTypeEnum.ProphecyManaged,
    ]
    dependsOnUpstreamSchema: bool = False

    @dataclass(frozen=True)
    class XMLParseProperties(MacroProperties):
        # properties for the component with default values
        columnName: str = ""
        relation_name: List[str] = field(default_factory=list)
        parsingMethod: str = "parseFromSampleRecord"
        sampleRecord: Optional[str] = None
        sampleSchema: Optional[str] = None
        # Snowflake: one output column per entry, as a JSON list of
        # {name, kind, tag?, instance?, attr?} (generated from sampleRecord or set by the transpiler).
        # The gem cannot tell the warehouse (SqlContext has no provider), so a Databricks gem may also
        # generate these from its sample record; the Databricks and DuckDB macros ignore them.
        outputColumns: str = ""
        includeSourceColumn: bool = True
        ignoreErrors: bool = True
        # Parse every element with this tag (one row each) instead of the root; Snowflake only.
        childElement: str = ""
        # UI only (not a macro argument): whether a sample record generates *_OuterXML columns.
        includeOuterXML: bool = False

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
        methodRadioGroup = (
            RadioGroup("Parsing method")
            .addOption(
                "Parse from sample record",
                "parseFromSampleRecord",
                description=("Provide a sample record to parse the schema from"),
            )
            .addOption(
                "Parse from schema",
                "parseFromSchema",
                description="Provide sample schema in SQL struct format to parse the data with",
            )
            .setOptionType("button")
            .setVariant("medium")
            .setButtonStyle("solid")
            .bindProperty("parsingMethod")
        )

        sampleRecordTextXML = (
            TextArea("Sample XML record to parse schema from", 20)
            .bindProperty("sampleRecord")
            .bindPlaceholder(
                """<root>
  <person>
    <id>1</id>
    <name>
      <first>John</first>
      <last>Doe</last>
    </name>
    <address>
      <street>Main St</street>
      <city>Springfield</city>
      <zip>12345</zip>
    </address>
  </person>
</root>"""
            )
        )
        return Dialog("ColumnParser").addElement(
            Condition()
            .ifEqual(PropExpr("$.sql.metainfo.providerType"), StringExpr("snowflake"))
            .then(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                    Ports(),
                    "content"
                )
                .addColumn(
                    StackLayout(height="100%")
                        .addElement(
                            StepContainer()
                            .addElement(
                                Step()
                                .addElement(
                                    StackLayout(height="100%")
                                    .addElement(
                                        SchemaColumnsDropdown("Select columns to parse",
                                                                appearance="minimal")
                                        .bindSchema("component.ports.inputs[0].schema")
                                        .bindProperty("columnName")
                                        .showErrorsFor("columnName")
                                    )
                                )
                            )
                        )
                        .addElement(
                            AlertBox(
                                variant="success",
                                _children=[
                                    Markdown(
                                        "With no output columns, **`XMLParse`** adds one VARIANT column with the suffix **`_parsed`**. "
                                        "Paste a sample record to get one string column per child element instead: "
                                        "the root value, each child's value (repeats as `<tag>_2`, `<tag>_3`, ...) and, "
                                        "optionally, each element's outer XML. With a specific child element, every "
                                        "element with that tag becomes its own row and is parsed in place of the root. "
                                        "Edited columns are kept when settings change; clear them to regenerate from the sample.\n"
                                    )
                                ]
                            )
                        )
                        .addElement(
                            StackLayout(gap="0.5rem")
                            .addElement(Checkbox("Include outer XML columns").bindProperty("includeOuterXML"))
                            .addElement(Checkbox("Keep the source XML column").bindProperty("includeSourceColumn"))
                            .addElement(Checkbox("Return nulls for malformed XML instead of failing").bindProperty("ignoreErrors"))
                        )
                        .addElement(
                            TextBox("Specific child element (leave empty to parse the root element)")
                            .bindPlaceholder("item")
                            .bindProperty("childElement")
                        )
                        .addElement(
                            TextArea("Sample XML record (generates the output columns)", 10)
                            .bindProperty("sampleRecord")
                            .bindPlaceholder("<order><id>1</id><item>A</item><item>B</item></order>")
                        )
                        .addElement(
                            TextArea("Output columns (JSON; edit to rename, drop or add attribute columns)", 10)
                            .bindProperty("outputColumns")
                            .bindPlaceholder(
                                '[{"name": "order", "kind": "root"}, '
                                '{"name": "id", "kind": "child", "tag": "id", "instance": 0}, '
                                '{"name": "item_2", "kind": "child", "tag": "item", "instance": 1}, '
                                '{"name": "id_type", "kind": "child_attr", "tag": "id", "attr": "type"}]'
                            )
                        )
                )
            )
            .otherwise(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(), "content")
                .addColumn(
                    StackLayout(height="100%")
                    .addElement(TitleElement("Select Column to Parse"))
                    .addElement(
                        StepContainer().addElement(
                            Step().addElement(
                                StackLayout(height="100%").addElement(
                                    SchemaColumnsDropdown("", appearance="minimal")
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("columnName")
                                    .showErrorsFor("columnName")
                                )
                            )
                        )
                    )
                    .addElement(methodRadioGroup)
                    .addElement(
                        Condition()
                        .ifEqual(
                            PropExpr("component.properties.parsingMethod"),
                            StringExpr("parseFromSampleRecord"),
                        )
                        .then(sampleRecordTextXML)
                    )
                    .addElement(
                        Condition()
                        .ifEqual(
                            PropExpr("component.properties.parsingMethod"),
                            StringExpr("parseFromSchema"),
                        )
                        .then(
                            TextArea("Schema struct to parse the column", 20)
                            .bindProperty("sampleSchema")
                            .bindPlaceholder(
                                """STRUCT<
  root: STRUCT<
    person: STRUCT<
      id: INT,
      name: STRUCT<
        first: STRING,
        last: STRING
      >,
      address: STRUCT<
        street: STRING,
        city: STRING,
        zip: INT
      >
    >
  >
>"""
                            )
                        )
                    ),
                    "1fr",
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(XMLParse, self).validate(context, component)

        if (
            component.properties.columnName is None
            or component.properties.columnName == ""
        ):
            diagnostics.append(
                Diagnostic(
                    "component.properties.columnName",
                    "Please select a column for the operation",
                    SeverityLevelEnum.Error,
                )
            )
        else:
            # Extract all column names from the schema
            field_names = [
                field["name"] for field in component.ports.inputs[0].schema["fields"]
            ]
            if component.properties.columnName not in field_names:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.columnName",
                        f"Selected column {component.properties.columnName} is not present in input schema.",
                        SeverityLevelEnum.Error,
                    )
                )

        # TODO: Removed null check validation as writing snowflake specific validation code is not possible in the current macro framework
        # ParseFromSchema and ParseFromSampleRecord are not applicable for snowflake

        if component.properties.parsingMethod == "parseFromSchema":
            if (
                component.properties.sampleSchema is None
                or component.properties.sampleSchema == ""
            ):
                diagnostics.append(
                    Diagnostic(
                        "component.properties.sampleSchema",
                        "Please provide a valid SQL struct schema",
                        SeverityLevelEnum.Error,
                    )
                )
        # TODO: Removed null check validation as writing snowflake specific validation code is not possible in the current macro framework
        # if component.properties.parsingMethod == "parseFromSampleRecord":
        #     if (
        #         component.properties.sampleRecord is None
        #         or component.properties.sampleRecord == ""
        #     ):
        #         diagnostics.append(
        #             Diagnostic(
        #                 "component.properties.sampleRecord",
        #                 "Please provide a valid sample xml record",
        #                 SeverityLevelEnum.Error,
        #             )
        #         )

        try:
            output_columns = parse_output_columns(component.properties.outputColumns)
        except ValueError as e:
            output_columns = []
            diagnostics.append(
                Diagnostic("component.properties.outputColumns", f"Output columns are not valid JSON: {e}", SeverityLevelEnum.Error)
            )
        for message in output_column_errors(output_columns):
            diagnostics.append(Diagnostic("component.properties.outputColumns", message, SeverityLevelEnum.Error))
        if output_columns:
            existing = set(self._input_column_names(component))
            if not component.properties.includeSourceColumn:
                existing.discard(component.properties.columnName)
            for c in output_columns:
                if isinstance(c, dict) and c.get("name") in existing:
                    diagnostics.append(
                        Diagnostic(
                            "component.properties.outputColumns",
                            f"Output column '{c['name']}' already exists in the input; rename it",
                            SeverityLevelEnum.Error,
                        )
                    )

        return diagnostics

    @staticmethod
    def _input_column_names(component: Component) -> List[str]:
        try:
            schema = component.ports.inputs[0].schema
            if isinstance(schema, str):  # NodePort.schema is typed str; the editor passes a dict
                schema = json.loads(schema.replace("'", '"'))
            return [f["name"] for f in schema["fields"]]
        except (AttributeError, IndexError, KeyError, TypeError, ValueError):
            return []

    def onChange(
        self, context: SqlContext, oldState: Component, newState: Component
    ) -> Component:
        # Handle changes in the newState's state and return the new state
        relation_name = self.get_relation_names(newState, context)
        newProperties = dataclasses.replace(
            newState.properties,
            relation_name=relation_name
        )

        # Regenerate the output columns from the sample when a setting they depend on changes, but
        # only while they are empty or still exactly what the previous settings generated: columns
        # someone edited (or the transpiler wrote) are never overwritten. Clearing them regenerates.
        old_key = self._generation_key(oldState, oldState.properties)
        new_key = self._generation_key(newState, newProperties)
        try:
            current = parse_output_columns(newProperties.outputColumns)
        except ValueError:
            current = None
        if new_key[0] and current is not None and (not current or (
                old_key != new_key and current == self._generated_columns(old_key))):
            columns = self._generated_columns(new_key)
            if columns is not None:
                newProperties = dataclasses.replace(newProperties, outputColumns=json.dumps(columns))
        return newState.bindProperties(newProperties)

    def _generation_key(self, state: Component, props) -> tuple:
        """Everything output_columns_from_sample depends on."""
        existing = self._input_column_names(state)
        if not props.includeSourceColumn:
            existing = [c for c in existing if c != props.columnName]
        return ((props.sampleRecord or "").strip(), tuple(existing), bool(props.includeOuterXML),
                (props.childElement or "").strip())

    @staticmethod
    def _generated_columns(key: tuple):
        sample, existing, include_outer_xml, child_element = key
        if not sample:
            return None
        try:
            return output_columns_from_sample(sample, list(existing), include_outer_xml, child_element)
        except ValueError:
            return None  # the sample is still being typed

    def apply(self, props: XMLParseProperties) -> str:
        # You can now access self.relation_name here
        resolved_macro_name = f"{self.projectName}.{self.name}"

        # Get the Single Table Name
        sampleRecord: str = props.sampleRecord if props.sampleRecord is not None else ""
        sampleSchema: str = props.sampleSchema if props.sampleSchema is not None else ""

        try:
            output_columns = parse_output_columns(props.outputColumns)
        except ValueError:
            # validate reports the bad list as an error; the call stays parseable for the SQL Editor
            output_columns = []

        arguments = [
            str(props.relation_name),
            _jinja_str(props.columnName),
            _jinja_str(props.parsingMethod),
            _jinja_str(sampleRecord),
            _jinja_str(sampleSchema),
            json.dumps(output_columns),  # a JSON list is a valid Jinja literal (no nulls are stored)
            "true" if props.includeSourceColumn else "false",
            "true" if props.ignoreErrors else "false",
            _jinja_str(props.childElement),
        ]
        params = ",".join([param for param in arguments])
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        try:
            loaded_columns = parse_output_columns(parametersMap.get("outputColumns"))
        except ValueError:
            loaded_columns = []  # kept as written, so validate reports it
        return XMLParse.XMLParseProperties(
            relation_name=json.loads(parametersMap.get('relation_name').replace("'", '"')),
            columnName=_decode_str(parametersMap.get("columnName")),
            parsingMethod=_decode_str(parametersMap.get("parsingMethod")),
            sampleRecord=_decode_str(parametersMap.get("sampleRecord")),
            sampleSchema=_decode_str(parametersMap.get("sampleSchema")),
            # read back from code the list may be a Python-style literal; keep JSON for the dialog
            outputColumns=(json.dumps(loaded_columns) if loaded_columns
                           else parametersMap.get("outputColumns") or ""),
            includeSourceColumn=_load_bool(parametersMap.get("includeSourceColumn"), True),
            ignoreErrors=_load_bool(parametersMap.get("ignoreErrors"), True),
            childElement=_decode_str(parametersMap.get("childElement")),
            includeOuterXML=any(
                isinstance(c, dict) and str(c.get("kind", "")).endswith("_outer") for c in loaded_columns
            ),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("columnName", properties.columnName),
                MacroParameter("parsingMethod", properties.parsingMethod),
                MacroParameter("sampleRecord", properties.sampleRecord),
                MacroParameter("sampleSchema", properties.sampleSchema),
                MacroParameter("outputColumns", properties.outputColumns or ""),
                MacroParameter("includeSourceColumn", "true" if properties.includeSourceColumn else "false"),
                MacroParameter("ignoreErrors", "true" if properties.ignoreErrors else "false"),
                MacroParameter("childElement", properties.childElement or ""),
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
        if self.props.parsingMethod == "parseFromSchema":
            if not self.props.sampleSchema or self.props.sampleSchema == "":
                res = in0
            else:
                schema_str = self.props.sampleSchema.replace("\n", " ").strip()
                quoted_col = f"`{self.props.columnName}`"
                alias_col = f"`{self.props.columnName}_parsed`"
                res = in0.selectExpr(
                    "*",
                    f"from_xml({quoted_col}, '{schema_str}') as {alias_col}"
                )

        elif self.props.parsingMethod == "parseFromSampleRecord":
            if not self.props.sampleRecord or self.props.sampleRecord == "":
                res = in0
            else:
                import re
                # Replace newlines and normalize whitespace
                sample_str = self.props.sampleRecord.replace("\n", " ").strip()
                sample_str = re.sub(r'\s+', ' ', sample_str)
                # Escape backslashes first, then double quotes
                sample_str_escaped = sample_str.replace("\\", "\\\\").replace('"', '\\"')
                
                quoted_col = f"`{self.props.columnName}`"
                alias_col = f"`{self.props.columnName}_parsed`"
                res = in0.selectExpr(
                    "*",
                    f"from_xml({quoted_col}, schema_of_xml(\"{sample_str_escaped}\")) as {alias_col}"
                )
        else:
            res = in0

        return res
