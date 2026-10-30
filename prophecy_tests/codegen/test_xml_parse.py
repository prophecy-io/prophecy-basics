"""XMLParse's output columns, serializer and loader.

On Snowflake the gem can emit one string column per child element (Alteryx's XML Parse) from an
`outputColumns` list, generated from a sample record or written by the transpiler. These tests
cover the column generation (Alteryx's names and order), validation, onChange, the call apply()
writes, and both load paths: the call's source text (code -> visual) and saved properties.

Requires the Prophecy component-builder SDK and Jinja2; no Spark session or warehouse.
Run: python -m pytest prophecy_tests/codegen
"""
import ast
import dataclasses
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "gems"))
from XMLParse import (XMLParse, BasicMacroProperties, MacroParameter, _jinja_str, output_column_errors,
                      output_columns_from_sample, parse_output_columns)
from prophecy.cb.sql.Component import Component, NodePort, NodePorts, SqlNodeMetadata
from prophecy.cb.sql.SqlContext import SqlContext, SqlGraph

def Props(**kwargs):
    """The gem's properties in "Parse into columns" mode unless a test says otherwise."""
    kwargs.setdefault("parsingMethod", "parseToColumns")
    return XMLParse.XMLParseProperties(**kwargs)

# The dbt macro signature, in order: the SQL Editor names positional arguments from it.
ARGUMENT_NAMES = ("relation_name", "columnName", "parsingMethod", "sampleRecord", "sampleSchema",
                  "outputColumns", "includeSourceColumn", "ignoreErrors", "childElement")

ORDER = ("<order><id>7</id><item>A</item><customer><name>N</name></customer>"
         "<item>B</item><item>C</item></order>")


def names(columns):
    return [c["name"] for c in columns]


def source_properties(call):
    # Scala WrapperMacro passes source tokens, not evaluated Python values.
    source = call[2:-2].strip()
    args = ast.parse(source, mode="eval").body.args
    return BasicMacroProperties(parameters=[
        MacroParameter(name, ast.get_source_segment(source, arg))
        for name, arg in zip(ARGUMENT_NAMES, args)
    ])


def rendered_args(call):
    received = []
    capture = SimpleNamespace(XMLParse=lambda *args: received.append(args) or "")
    Environment().from_string(call).render(prophecy_basics=capture)
    return received[0]


def component(props, input_columns=("id", "payload")):
    # NodePort.schema is typed str, but the SQL Editor hands gems the parsed dict (validate indexes it)
    schema = {"fields": [{"name": c, "dataType": {"type": "String"}} for c in input_columns]}
    return Component(id="xml", component="XMLParse", metadata=SqlNodeMetadata(label="XMLParse_1"),
                     ports=NodePorts(inputs=[NodePort(id="p0", slug="in0", schema=schema)], outputs=[]),
                     properties=props)


CONTEXT = SqlContext(graph=SqlGraph(connections=[], nodes={}), projectName="prophecy_basics",
                     projectMacros=[], dependencyProjectMacros={})


# ---- output columns from a sample record -------------------------------------------------------

def test_root_first_then_each_childs_first_occurrence_then_repeats():
    cols = output_columns_from_sample(ORDER, ["payload"])
    assert names(cols) == ["order", "id", "item", "customer", "item_2", "item_3"]
    assert cols[0] == {"kind": "root", "name": "order"}
    assert cols[4] == {"kind": "child", "tag": "item", "instance": 1, "name": "item_2"}


def test_outer_xml_follows_each_child_and_the_roots_comes_before_the_repeats():
    cols = output_columns_from_sample(ORDER, ["payload"], include_outer_xml=True)
    assert names(cols) == [
        "order", "id", "id_OuterXML", "item", "item_OuterXML", "customer", "customer_OuterXML",
        "order_OuterXML", "item_2", "item_2_OuterXML", "item_3", "item_3_OuterXML",
    ]
    assert {"kind": "root_outer", "name": "order_OuterXML"} in cols
    assert {"kind": "child_outer", "tag": "item", "instance": 2, "name": "item_3_OuterXML"} in cols


def test_a_name_already_in_the_input_gets_alteryxs_numeric_suffix():
    cols = output_columns_from_sample(ORDER, ["order", "id", "id2", "order_OuterXML"], include_outer_xml=True)
    by_name = {c["name"]: c for c in cols}
    assert by_name["order2"]["kind"] == "root"
    assert by_name["id3"] == {"kind": "child", "tag": "id", "instance": 0, "name": "id3"}
    assert by_name["order_OuterXML2"]["kind"] == "root_outer"


LISTING = ('<doc><meta><n>1</n></meta><list><row id="1"><v>a</v></row>'
           '<row id="2"><v>b</v><v>c</v><row id="3"><v>d</v></row></row></list></doc>')


def test_a_child_element_is_parsed_in_place_of_the_root_from_its_first_occurrence():
    cols = output_columns_from_sample(LISTING, ["payload"], include_outer_xml=True, child_element="row")
    assert names(cols) == ["row", "v", "v_OuterXML", "row_OuterXML"]
    assert cols[0] == {"kind": "root", "name": "row"}


def test_a_child_element_is_found_at_any_depth_but_never_as_the_root():
    assert names(output_columns_from_sample(LISTING, [], child_element="n")) == ["n"]
    with pytest.raises(ValueError, match="no <doc> element"):
        output_columns_from_sample(LISTING, [], child_element="doc")
    with pytest.raises(ValueError, match="no <missing> element"):
        output_columns_from_sample(LISTING, [], child_element="missing")


def test_a_namespaced_sample_is_refused_rather_than_guessed():
    with pytest.raises(ValueError, match="namespaces"):
        output_columns_from_sample('<o xmlns:p="urn:x"><p:id>1</p:id></o>', [])


def test_a_malformed_sample_is_an_error():
    with pytest.raises(ValueError):
        output_columns_from_sample("<order><id>1</order>", [])


# ---- parsing and checking the list -------------------------------------------------------------

def test_an_empty_list_and_malformed_lists():
    assert parse_output_columns("") == [] and parse_output_columns(None) == []
    with pytest.raises(ValueError):
        parse_output_columns('{"name": "x"}')
    with pytest.raises(ValueError):
        parse_output_columns("[not json")


def test_a_python_style_literal_is_accepted():
    assert parse_output_columns("[{'name': 'id', 'kind': 'child', 'tag': 'id', 'instance': 0}]") == [
        {"name": "id", "kind": "child", "tag": "id", "instance": 0}]


def test_each_entry_is_checked():
    assert output_column_errors([
        {"name": "a", "kind": "child", "tag": "a"},
        {"name": "a", "kind": "root"},
        {"name": "b", "kind": "child"},
        {"name": "c", "kind": "child_attr", "tag": "c"},
        {"name": "d", "kind": "grandchild"},
        {"name": "e", "kind": "child", "tag": "e", "instance": -1},
        {"kind": "root"},
    ]) == [
        "Output column name 'a' is used more than once",
        "Output column 'b' needs a tag",
        "Output column 'c' needs an attr",
        "Output column 'd' has unknown kind 'grandchild'; expected one of root, root_outer, root_attr, child, child_outer, child_attr",
        "Output column 'e' instance must be a non-negative integer",
        "Output column 7 has no name",
    ]


# ---- validate ----------------------------------------------------------------------------------

def messages(props, input_columns=("id", "payload")):
    """The gem's own diagnostics on outputColumns (the SDK's base checks need the project's macros)."""
    return [d.message for d in XMLParse().validate(CONTEXT, component(props, input_columns))
            if d.path == "component.properties.outputColumns"]


def test_generated_columns_validate():
    cols = output_columns_from_sample(ORDER, ["id", "payload"], include_outer_xml=True)
    assert messages(Props(columnName="payload", outputColumns=json.dumps(cols))) == []


def test_malformed_json_and_a_clash_with_the_input_are_reported():
    assert any("not valid JSON" in m for m in messages(Props(columnName="payload", outputColumns="[")))
    clash = json.dumps([{"name": "id", "kind": "child", "tag": "id"}])
    assert messages(Props(columnName="payload", outputColumns=clash)) == [
        "Output column 'id' already exists in the input; rename it"]


def test_the_column_settings_are_not_validated_in_the_other_modes():
    assert messages(Props(columnName="payload", parsingMethod="parseFromSampleRecord", outputColumns="[")) == []


def test_the_source_columns_name_is_free_once_the_source_is_dropped():
    reuse = json.dumps([{"name": "payload", "kind": "root"}])
    assert messages(Props(columnName="payload", outputColumns=reuse)) == [
        "Output column 'payload' already exists in the input; rename it"]
    assert messages(Props(columnName="payload", outputColumns=reuse, includeSourceColumn=False)) == []


# ---- onChange ----------------------------------------------------------------------------------

def changed(old_props, new_props):
    return XMLParse().onChange(CONTEXT, component(old_props), component(new_props)).properties


def test_a_new_sample_generates_columns_that_avoid_the_input_names():
    p = changed(Props(columnName="payload"), Props(columnName="payload", sampleRecord=ORDER))
    assert names(parse_output_columns(p.outputColumns)) == ["order", "id2", "item", "customer", "item_2", "item_3"]


def test_hand_edited_columns_are_kept_while_the_sample_is_unchanged():
    edited = json.dumps([{"name": "order_id", "kind": "child", "tag": "id"}])
    old = Props(columnName="payload", sampleRecord=ORDER, outputColumns=edited)
    assert changed(old, dataclasses.replace(old, ignoreErrors=False)).outputColumns == edited


def test_turning_on_outer_xml_regenerates_the_columns():
    old = Props(columnName="payload", sampleRecord=ORDER)
    assert "order_OuterXML" in names(parse_output_columns(
        changed(old, dataclasses.replace(old, includeOuterXML=True)).outputColumns))


def test_choosing_a_child_element_regenerates_the_columns():
    old = Props(columnName="payload", sampleRecord=LISTING)
    p = changed(old, dataclasses.replace(old, childElement="row"))
    assert names(parse_output_columns(p.outputColumns)) == ["row", "v"]


def generated(sample, outer=False, input_columns=("id", "payload")):
    return json.dumps(output_columns_from_sample(sample, list(input_columns), include_outer_xml=outer))


def test_generated_columns_follow_the_outer_xml_setting():
    old = Props(columnName="payload", sampleRecord=ORDER, outputColumns=generated(ORDER))
    p = changed(old, dataclasses.replace(old, includeOuterXML=True))
    assert p.outputColumns == generated(ORDER, outer=True)


def test_edited_columns_survive_changes_to_the_settings_they_came_from():
    edited = json.dumps([{"name": "order_id", "kind": "child", "tag": "id", "instance": 0}])
    old = Props(columnName="payload", sampleRecord=ORDER, outputColumns=edited)
    for new in (dataclasses.replace(old, includeOuterXML=True),
                dataclasses.replace(old, includeSourceColumn=False),
                dataclasses.replace(old, childElement="item"),
                dataclasses.replace(old, sampleRecord="<order><x>1</x></order>")):
        assert changed(old, new).outputColumns == edited


def test_clearing_edited_columns_regenerates_them():
    edited = Props(columnName="payload", sampleRecord=ORDER, outputColumns='[{"name": "x", "kind": "root"}]')
    p = changed(edited, dataclasses.replace(edited, outputColumns=""))
    assert p.outputColumns == generated(ORDER)


def test_transpiler_written_columns_without_a_sample_are_never_touched():
    cols = json.dumps([{"name": "id2", "kind": "child", "tag": "id", "instance": 0}])
    old = Props(columnName="payload", outputColumns=cols)
    assert changed(old, dataclasses.replace(old, includeOuterXML=True, childElement="item")).outputColumns == cols


def type_into(gem_props, field, text, input_columns=("id", "payload")):
    """Feed onChange one keystroke at a time, as the dialog does."""
    gem, p = XMLParse(), gem_props
    for i in range(1, len(text) + 1):
        p = gem.onChange(CONTEXT, component(p, input_columns),
                         component(dataclasses.replace(p, **{field: text[:i]}), input_columns)).properties
    return p


def test_retyping_the_sample_one_key_at_a_time_ends_with_its_columns():
    """Every prefix of a sample is malformed XML; the columns must not freeze on the old sample."""
    start = type_into(Props(columnName="payload"), "sampleRecord", "<o><id>1</id></o>")
    assert names(parse_output_columns(start.outputColumns)) == ["o", "id2"]
    retyped = type_into(dataclasses.replace(start, sampleRecord=""), "sampleRecord", "<x><y>1</y><y>2</y></x>")
    assert names(parse_output_columns(retyped.outputColumns)) == ["x", "y", "y_2"]
    toggled = changed(retyped, dataclasses.replace(retyped, includeOuterXML=True))
    assert "x_OuterXML" in names(parse_output_columns(toggled.outputColumns))


def test_typing_a_child_element_name_ends_with_its_columns():
    start = Props(columnName="payload", sampleRecord=LISTING)
    p = type_into(start, "childElement", "row")
    assert names(parse_output_columns(p.outputColumns)) == ["row", "v"]


def test_generated_columns_follow_a_new_clashing_input_column_and_stay_valid():
    """A Databricks gem never shows outputColumns; generated ones must never fail validation."""
    gem = XMLParse()
    p = type_into(Props(columnName="payload"), "sampleRecord", "<o><v>1</v></o>", input_columns=("payload",))
    assert names(parse_output_columns(p.outputColumns)) == ["o", "v"]
    p = gem.onChange(CONTEXT, component(p, ("payload",)), component(p, ("payload", "v"))).properties
    assert names(parse_output_columns(p.outputColumns)) == ["o", "v2"]
    assert messages(p, ("payload", "v")) == []


def test_the_generation_bookkeeping_is_saved_but_never_in_the_call():
    p = type_into(Props(relation_name=["src"], columnName="payload"), "sampleRecord", "<o><v>1</v></o>")
    assert p.generatedColumns == p.outputColumns
    assert "generatedColumns" not in XMLParse().apply(p)
    assert XMLParse().loadProperties(XMLParse().unloadProperties(p)).generatedColumns == p.generatedColumns


@pytest.mark.parametrize("mode", ["parseFromSampleRecord", "parseFromSchema", ""])
def test_a_gem_in_another_mode_comes_back_from_the_editor_unchanged(mode):
    """Opening a gem from before Parse into columns existed must not add columns to its call: on
    Snowflake that would switch its output from the VARIANT column to separate columns."""
    p = Props(relation_name=["src"], columnName="payload", parsingMethod=mode, sampleRecord=ORDER)
    q = XMLParse().onChange(CONTEXT, component(p), component(p)).properties
    q = dataclasses.replace(q, relation_name=p.relation_name)  # onChange re-reads inputs from the (empty) graph
    assert q.outputColumns == "" and XMLParse().apply(q) == XMLParse().apply(p)


def test_switching_to_parse_into_columns_generates_the_columns():
    old = Props(columnName="payload", parsingMethod="parseFromSampleRecord", sampleRecord=ORDER)
    assert changed(old, dataclasses.replace(old, parsingMethod="parseToColumns")).outputColumns == generated(ORDER)


def test_a_sample_still_being_typed_leaves_the_columns_alone():
    old = Props(columnName="payload", outputColumns='[{"name": "order", "kind": "root"}]')
    assert changed(old, dataclasses.replace(old, sampleRecord="<order><id>")).outputColumns == old.outputColumns


# ---- apply and the two load paths --------------------------------------------------------------

TRICKY = ["plain", "it's", 'say "hi"', "back\\slash", "<a b='1'>c:\\d</a>", "é漢字"]


def props_with(text):
    cols = [{"name": f"{text}_col", "kind": "child", "tag": "item", "instance": 1}]
    return Props(relation_name=["Source_1"], columnName=text, parsingMethod="parseFromSampleRecord",
                 sampleRecord=text, sampleSchema="", outputColumns=json.dumps(cols),
                 includeSourceColumn=False, ignoreErrors=False, childElement=text)


def test_column_names_outside_the_basic_plane_reach_the_macro_intact():
    """json.dumps' default \\ud83d\\ude00 escapes are decoded half by half by Jinja."""
    cols = [{"name": "rate 😀", "kind": "child", "tag": "r𝔵", "instance": 0}]
    p = Props(relation_name=["src"], columnName="payload", outputColumns=json.dumps(cols))
    assert rendered_args(XMLParse().apply(p))[5] == cols


@pytest.mark.parametrize("value", ["''", "'quoted'", '"double"'])
def test_a_saved_value_that_looks_like_a_literal_is_kept_as_is(value):
    """Saved properties hold the value itself, never source text: nothing is decoded."""
    p = Props(relation_name=["src"], columnName=value, sampleRecord=value, childElement=value)
    q = XMLParse().loadProperties(XMLParse().unloadProperties(p))
    assert (q.columnName, q.sampleRecord, q.childElement) == (value, value, value)


def test_a_legacy_gem_writes_an_empty_column_list_and_the_defaults():
    assert XMLParse().apply(Props(relation_name=["src"], columnName="payload", parsingMethod="parseFromSampleRecord")) == (
        "{{ prophecy_basics.XMLParse(['src'],'payload','parseFromSampleRecord','','',[],true,true,'') }}")


@pytest.mark.parametrize("text", TRICKY)
def test_the_generated_call_renders_the_exact_values(text):
    args = rendered_args(XMLParse().apply(props_with(text)))
    assert args == (["Source_1"], text, "parseFromSampleRecord", text, "",
                    [{"name": f"{text}_col", "kind": "child", "tag": "item", "instance": 1}], False, False, text)


@pytest.mark.parametrize("text", TRICKY)
def test_code_visual_save_cycles_change_nothing(text):
    gem, p = XMLParse(), props_with(text)
    first = gem.apply(p)
    for _ in range(5):
        p = gem.loadProperties(source_properties(gem.apply(p)))
        assert (p.columnName, p.sampleRecord, p.includeSourceColumn, p.ignoreErrors, p.childElement) == (
            text, text, False, False, text)
        assert parse_output_columns(p.outputColumns) == parse_output_columns(props_with(text).outputColumns)
    assert gem.apply(p) == first


@pytest.mark.parametrize("text", TRICKY)
def test_saved_properties_load_back_unchanged(text):
    gem, p = XMLParse(), props_with(text)
    for _ in range(5):
        p = gem.loadProperties(gem.unloadProperties(p))
    assert p == props_with(text)


def test_outer_xml_is_restored_from_the_column_list():
    cols = json.dumps(output_columns_from_sample(ORDER, [], include_outer_xml=True))
    p = XMLParse().loadProperties(XMLParse().unloadProperties(Props(columnName="payload", outputColumns=cols)))
    assert p.includeOuterXML is True


def test_a_call_written_before_output_columns_existed_still_loads():
    old = "{{ prophecy_basics.XMLParse(['src'],'payload','parseFromSampleRecord','','') }}"
    p = XMLParse().loadProperties(source_properties(old))
    assert (p.relation_name, p.columnName, p.parsingMethod, p.outputColumns) == (["src"], "payload", "parseFromSampleRecord", "")
    assert (p.includeSourceColumn, p.ignoreErrors, p.includeOuterXML, p.childElement) == (True, True, False, "")


def test_a_python_style_list_read_back_from_code_is_stored_as_json():
    call = ("{{ prophecy_basics.XMLParse(['src'],'payload','','','',"
            "[{'name': 'id', 'kind': 'child', 'tag': 'id', 'instance': 0}],true,true,'') }}")
    p = XMLParse().loadProperties(source_properties(call))
    assert json.loads(p.outputColumns) == [{"name": "id", "kind": "child", "tag": "id", "instance": 0}]


def test_a_multi_line_sample_reads_back_from_its_literal_unchanged():
    """The SQL Editor hands loadProperties the literal apply wrote; a pasted sample spans lines."""
    sample = '<order>\n  <id it\'s="1">c:\\d</id>\n</order>'
    literal = _jinja_str(sample)
    assert "\n" in literal  # written as is: Jinja string literals may span lines
    p = XMLParse().loadProperties(BasicMacroProperties(parameters=[
        MacroParameter("relation_name", "['src']"), MacroParameter("columnName", "'payload'"),
        MacroParameter("parsingMethod", "''"), MacroParameter("sampleRecord", literal),
        MacroParameter("sampleSchema", "''")]))
    assert p.sampleRecord == sample
    assert rendered_args(XMLParse().apply(p))[3] == sample


def test_settings_saved_before_output_columns_existed_still_load():
    old = BasicMacroProperties(parameters=[
        MacroParameter("relation_name", '["src"]'), MacroParameter("columnName", "payload"),
        MacroParameter("parsingMethod", "parseFromSchema"), MacroParameter("sampleRecord", None),
        MacroParameter("sampleSchema", "STRUCT<a: STRING>")])
    p = XMLParse().loadProperties(old)
    assert (p.columnName, p.sampleRecord, p.sampleSchema, p.outputColumns, p.ignoreErrors) == (
        "payload", "", "STRUCT<a: STRING>", "", True)


def test_loading_does_not_print_the_parameters(capsys):
    """The parameters include the whole sample record, which can be customer data."""
    XMLParse().loadProperties(XMLParse().unloadProperties(props_with("it's")))
    assert capsys.readouterr().out == ""


# ---- the Databricks macro's SQL literals -------------------------------------------------------

def _default_macro(**kwargs):
    sql = (ROOT / "macros" / "XMLParse.sql").read_text()
    env = Environment()
    env.globals.update(log=lambda *a, **k: "",
                       exceptions=SimpleNamespace(raise_compiler_error=lambda m: (_ for _ in ()).throw(ValueError(m))))
    args = dict(relation_name=["src"], columnName="payload", parsingMethod="parseFromSampleRecord",
                sampleRecord="", sampleSchema="")
    args.update(kwargs)
    call = "{{ default__XMLParse(" + ", ".join(f"{k}={v!r}" for k, v in args.items()) + ") }}"
    return " ".join(env.from_string(sql + call).render().split())


SPARK_SAMPLE = "<a b='1'>c:\\d\nit's</a>"


def test_the_databricks_sample_record_is_a_valid_spark_sql_literal():
    sql = _default_macro(sampleRecord=SPARK_SAMPLE)
    assert "schema_of_xml('<a b=\\'1\\'>c:\\\\d it\\'s</a>')" in sql


def test_the_databricks_schema_is_a_valid_spark_sql_literal():
    sql = _default_macro(parsingMethod="parseFromSchema", sampleSchema="STRUCT<`it's`: STRING>")
    assert "from_xml(`payload`, 'STRUCT<`it\\'s`: STRING>')" in sql


# ---- Databricks: parsingMethod "parseToColumns" ------------------------------------------------

def _dialog_text():
    return json.dumps(XMLParse().dialog().json())


def test_the_databricks_dialog_offers_parse_into_columns_with_the_column_settings():
    text = _dialog_text()
    assert '"parseToColumns"' in text
    # the column settings appear twice: the Snowflake dialog and Databricks' Parse into columns
    for prop in ("outputColumns", "childElement", "includeOuterXML", "includeSourceColumn", "ignoreErrors"):
        assert text.count(f"component.properties.{prop}") == 2, prop


def test_parse_into_columns_round_trips_through_code():
    p = dataclasses.replace(props_with("payload"), parsingMethod="parseToColumns")
    q = XMLParse().loadProperties(source_properties(XMLParse().apply(p)))
    assert q.parsingMethod == "parseToColumns" and XMLParse().apply(q) == XMLParse().apply(p)


def test_the_python_runtime_refuses_parse_into_columns_rather_than_return_the_input():
    gem = XMLParse()
    gem.props = Props(columnName="payload", parsingMethod="parseToColumns")
    with pytest.raises(ValueError, match="Parse into columns"):
        gem.applyPython(None, None)


def _snowflake_macro(**kwargs):
    sql = (ROOT / "macros" / "XMLParse.sql").read_text()
    env = Environment()
    env.globals.update(exceptions=SimpleNamespace(raise_compiler_error=lambda m: (_ for _ in ()).throw(ValueError(m))))
    args = dict(relation_name=["src"], columnName="payload", parsingMethod="parseToColumns", sampleRecord="",
                sampleSchema="", outputColumns=[{"name": "id", "kind": "child", "tag": "id", "instance": 0}])
    args.update(kwargs)
    call = "{{ snowflake__XMLParse(" + ", ".join(f"{k}={v!r}" for k, v in args.items()) + ") }}"
    return " ".join(env.from_string(sql + call).render().split())


def test_snowflake_uses_the_columns_only_in_parse_into_columns():
    assert "XMLGET" in _snowflake_macro()
    for mode in ("parseFromSampleRecord", "", "parseFromSchema"):
        sql = _snowflake_macro(parsingMethod=mode)
        assert "XMLGET" not in sql and 'PARSE_XML("payload") as "payload_parsed"' in sql, mode


def test_databricks_uses_the_columns_only_in_parse_into_columns():
    cols = [{"name": "id", "kind": "child", "tag": "id", "instance": 0}]
    assert "xpath_string" in _default_macro(parsingMethod="parseToColumns", outputColumns=cols)
    assert "xpath_string" not in _default_macro(sampleRecord="<a/>", outputColumns=cols)
    assert _default_macro(parsingMethod="parseToColumns") == "select * from src"

