"""Exercise the actual gem serializer and loader against Jinja's string semantics.

Requires the Prophecy component-builder SDK and Jinja2; no Spark session or
warehouse connection is used. Run: python -m pytest prophecy_tests/codegen
"""
import ast
from dataclasses import replace
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "gems"))
from MultiColumnRename import MultiColumnRename, BasicMacroProperties, MacroParameter

SCHEMA = json.dumps([{"name": "ROWCOUNT"}])
ARGUMENT_NAMES = ("relation_name", "columnNames", "renameMethod", "allColumnNames",
                  "editType", "editWith", "customExpression")
EXPRESSIONS = [
    r"(REGEXP_REPLACE(column_name, '\\W', '_'))",
    "UPPER(column_name)",
    "REPLACE(column_name, 'a', '_')",
    'REPLACE(column_name, \'"\', \'_\')',
    r"REPLACE(column_name, '\\n\\t\\b', '_')",
    "REPLACE(\n\tcolumn_name, 'a', '_')",
    "REPLACE(column_name, 'é漢字😀', '_')",
    "", '"column_name"', '\\"column_name\\"',
]

def properties(expression):
    return MultiColumnRename.MultiColumnRenameProperties(
        relation_name=["input"], columnNames=["ROWCOUNT"], schema=SCHEMA,
        renameMethod="advancedRename", editType="Suffix", editWith="",
        customExpression=expression,
    )

def source_properties(call):
    # Scala WrapperMacro passes source tokens, not evaluated Python values.
    source = call[2:-2].strip()
    args = ast.parse(source, mode="eval").body.args
    return BasicMacroProperties(parameters=[
        MacroParameter(name, ast.get_source_segment(source, arg))
        for name, arg in zip(ARGUMENT_NAMES, args)
    ] + [MacroParameter("schema", SCHEMA)])

def rendered_expression(call):
    received = []
    capture = SimpleNamespace(MultiColumnRename=lambda *args: received.append(args) or "")
    Environment().from_string(call).render(prophecy_basics=capture)
    return received[0][-1]

@pytest.mark.parametrize("expression", EXPRESSIONS)
def test_generated_jinja_preserves_exact_sql_expression(expression):
    gem = MultiColumnRename()
    assert rendered_expression(gem.apply(properties(expression))) == expression

@pytest.mark.parametrize("expression", EXPRESSIONS)
@pytest.mark.parametrize("method", ["", "editPrefixSuffix", "advancedRename"])
def test_code_visual_save_cycles_do_not_add_or_remove_escaping(expression, method):
    gem = MultiColumnRename()
    props = replace(properties(expression), renameMethod=method)
    for _ in range(5):
        call = gem.apply(props)
        props = gem.loadProperties(source_properties(call))
        assert props.renameMethod == method
        assert props.customExpression == expression
        assert rendered_expression(gem.apply(props)) == expression

@pytest.mark.parametrize("expression", EXPRESSIONS)
@pytest.mark.parametrize("method", ["", "editPrefixSuffix", "advancedRename"])
def test_legacy_unloaded_properties_remain_raw_including_quoted_sql(expression, method):
    gem = MultiColumnRename()
    props = replace(properties(expression), renameMethod=method)
    for _ in range(5):
        props = gem.loadProperties(gem.unloadProperties(props))
        assert props.renameMethod == method
        assert props.customExpression == expression

@pytest.mark.parametrize("token", ["'\\\\W'", '"column_name"', "'é😀'", "'a' 'b'"])
def test_loader_preserves_meaning_of_handwritten_jinja_constants(token):
    gem = MultiColumnRename()
    macro = source_properties(gem.apply(properties("unused")))
    for parameter in macro.parameters:
        if parameter.name == "customExpression": parameter.value = token
    expected = Environment().compile_expression(token)()
    assert gem.loadProperties(macro).customExpression == expected

@pytest.mark.parametrize("token", ["var('expression')", "column_name", "7", "none", "'x' ~ var('suffix')"])
def test_nonconstant_source_is_rejected_instead_of_converted_to_literal_sql(token):
    gem = MultiColumnRename()
    macro = source_properties(gem.apply(properties("unused")))
    for parameter in macro.parameters:
        if parameter.name == "customExpression": parameter.value = token
    with pytest.raises(ValueError):
        gem.loadProperties(macro)


class ExtraRename(MultiColumnRename):
    # Register a choice once; both dialog construction and loading must see it.
    RENAME_METHODS = {"newRename": "New rename"}


def rename_options(gem):
    def walk(value):
        if isinstance(value, dict):
            yield value
            for child in value.values():
                yield from walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk(child)

    selectors = [node for node in walk(gem.dialog().json())
                 if node.get("kind") == "Atoms.SelectBox"
                 and node["properties"].get("value") == "${component.properties.renameMethod}"]
    assert len(selectors) == 1
    return selectors[0]["properties"]["options"]


def test_current_rename_choices_keep_their_labels_and_order():
    assert rename_options(MultiColumnRename()) == [
        {"label": "Edit prefix/suffix", "value": "editPrefixSuffix"},
        {"label": "Advanced rename", "value": "advancedRename"},
    ]


def test_registered_rename_method_appears_in_dialog():
    assert rename_options(ExtraRename()) == [{"label": "New rename", "value": "newRename"}]


@pytest.mark.parametrize("source_format", [False, True], ids=["saved-properties", "generated-code"])
@pytest.mark.parametrize("expression", [EXPRESSIONS[0], '"column_name"'])
def test_registered_rename_method_loads_without_a_separate_allowlist(source_format, expression):
    gem = ExtraRename()
    props = replace(properties(expression), renameMethod="newRename")
    macro = source_properties(gem.apply(props)) if source_format else gem.unloadProperties(props)
    loaded = gem.loadProperties(macro)
    assert loaded.renameMethod == "newRename"
    assert loaded.customExpression == expression
    assert rendered_expression(gem.apply(loaded)) == expression
