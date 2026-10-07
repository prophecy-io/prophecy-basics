"""ToDo's serializer and loader against what the SQL Editor actually hands them.

A transpiled ToDo broke on every code -> visual cycle: its message gained a pair of quotes each
time (ToDo(''msg'') then no longer compiles), and -- because the call named no input -- the
editor rebuilt the gem with no input and moved the CTEs before it into a model of their own,
losing the edge. "Error message" and "Helper code/text" stay out of the call (the helper code is
the source tool's XML); they survive only through the gem's saved properties.

Requires the Prophecy component-builder SDK and Jinja2; no Spark session or warehouse.
Run: python -m pytest prophecy_tests/codegen
"""
import ast
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "gems"))
from ToDo import ToDo, BasicMacroProperties, MacroParameter
from prophecy.cb.sql.Component import Component, NodePort, NodePorts, SqlNodeMetadata
from prophecy.cb.sql.SqlContext import SqlContext, SqlGraph, NodeConnection

# The dbt macro signature, in order: the SQL Editor names positional arguments from it.
ARGUMENT_NAMES = ("diag_message", "relation_name")
CODE = ('<Node ToolID="29">\n  <GuiSettings Plugin="PortfolioComposerText"/>\n'
        '  <Value name="Text">it\'s "quoted" & <b>bold</b>, \\n not a newline, é漢字😀</Value>\n</Node>')
MESSAGES = [
    "Component type: Report Text is not supported.",
    "Report Text isn't supported",
    'say "hi"',
    "back\\slash and a real\nnewline\tand tab",
    "é漢字😀",
    "'already quoted'",
]


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
    capture = SimpleNamespace(ToDo=lambda *args: received.append(args) or "")
    Environment().from_string(call).render(prophecy_basics=capture)
    return received[0]


def props(message, relations=("AlteryxSelect_28",), error="Report Text has no SQL form", code=CODE):
    return ToDo.ToDoProperties(relation_name=list(relations), diag_message=message,
                               error_string=error, code_string=code)


@pytest.mark.parametrize("message", MESSAGES)
def test_the_generated_call_renders_the_exact_values(message):
    args = rendered_args(ToDo().apply(props(message)))
    assert args == (message, ["AlteryxSelect_28"])


@pytest.mark.parametrize("message", MESSAGES)
def test_code_visual_save_cycles_change_nothing(message):
    gem, p = ToDo(), props(message)
    first = gem.apply(p)
    for _ in range(5):
        p = gem.loadProperties(source_properties(gem.apply(p)))
        assert p == props(message, error=None, code=None)  # the call carries message + inputs
    assert gem.apply(p) == first  # no quotes or escapes added per cycle


def test_the_call_names_the_gems_inputs():
    """The SQL Editor makes a macro gem's inputs from the call's arguments whose value is an
    earlier CTE (ProcessVisualGen.getSourcesFromMacro); with none, the gem comes back with no
    input and the CTEs feeding it move to a model of their own."""
    call = ToDo().apply(props("m", relations=("Join_26_inner", "AlteryxSelect_28")))
    assert source_properties(call).parameters[1].value == "['Join_26_inner', 'AlteryxSelect_28']"


def test_a_call_written_by_the_old_gem_still_loads():
    old = "{{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}"
    p = ToDo().loadProperties(source_properties(old))
    assert p.diag_message == "Component type: Report Text is not supported."
    assert p.relation_name == [] and p.error_string is None and p.code_string is None


def test_a_message_the_old_gem_already_double_quoted_is_healed():
    """ToDo(''msg'') -- what the old load/apply cycle wrote -- does not even compile."""
    broken = BasicMacroProperties(parameters=[
        MacroParameter("diag_message", "''Component type: Report Text is not supported.''")])
    p = ToDo().loadProperties(broken)
    assert p.diag_message == "Component type: Report Text is not supported."
    assert rendered_args(ToDo().apply(p))[0] == "Component type: Report Text is not supported."


@pytest.mark.parametrize("message", [m for m in MESSAGES if not m.startswith("'")])
def test_unloaded_properties_load_back_unchanged(message):
    gem, p = ToDo(), props(message)
    for _ in range(5):
        p = gem.loadProperties(gem.unloadProperties(p))
    assert p == props(message)


@pytest.mark.parametrize("message", ["'already quoted'", '"double quoted"', "''looks broken''"])
def test_a_saved_message_that_looks_like_a_literal_is_kept_as_is(message):
    """Saved properties hold the value itself, never source text: nothing is decoded or healed."""
    p = ToDo().loadProperties(ToDo().unloadProperties(props(message)))
    assert p.diag_message == message


@pytest.mark.parametrize("message", ["''looks broken''", "'already quoted'"])
def test_a_message_that_looks_like_a_literal_survives_the_code_path(message):
    """apply writes a valid literal, so the repair of a broken ToDo(''msg'') never touches it."""
    gem = ToDo()
    p = gem.loadProperties(source_properties(gem.apply(props(message))))
    assert p.diag_message == message


def test_settings_saved_by_the_old_gem_still_load():
    """1.0.17's unloadProperties saved only the raw message."""
    old = BasicMacroProperties(parameters=[MacroParameter("diag_message", "Component type: Report Text")])
    p = ToDo().loadProperties(old)
    assert p.diag_message == "Component type: Report Text" and p.relation_name == []


def test_error_and_helper_code_stay_out_of_the_sql():
    """The helper code is the source tool's XML: kilobytes inline in the model, on every ToDo."""
    call = ToDo().apply(props("m"))
    assert call == "{{ prophecy_basics.ToDo(\"m\", ['AlteryxSelect_28']) }}"


def _macro_env():
    sql = (ROOT / "macros" / "ToDo.sql").read_text()
    env = Environment()
    env.globals.update(adapter=SimpleNamespace(dispatch=lambda name, package: lambda msg: f"<{msg}>"),
                       **{"return": lambda value: value})
    return env, sql


@pytest.mark.parametrize("call, message", [
    ("ToDo('legacy one-argument call')", "legacy one-argument call"),
    ("ToDo(\"m\", ['AlteryxSelect_28'])", "m"),
])
def test_the_dbt_macro_accepts_old_and_new_calls(call, message):
    env, sql = _macro_env()
    out = env.from_string(sql + "{{ " + call + " }}").render().strip()
    assert out == f"<{message}>"  # only the message reaches the adapter's SQL


# ---- several inputs ---------------------------------------------------------------------------
# How the editor names the CTE a gem reads (MultiPortSlugUtils): the upstream gem's label, or
# `<label>_<port>` when the upstream has several outputs -- and that per-port name is the input
# port's slug. Single-output and unconnected inputs keep the default slug `in<i>`.

def _onchange(ports):
    """Run the gem's onChange on a graph: `ports` is [(slug, upstream label or None)]."""
    inputs, nodes, connections = [], {}, []
    for i, (slug, upstream) in enumerate(ports):
        inputs.append(NodePort(id=f"p{i}", slug=slug, schema='{"fields": []}'))
        if upstream is not None:
            nodes[f"n{i}"] = SqlNodeMetadata(label=upstream)
            connections.append(NodeConnection(id=f"c{i}", source=f"n{i}", sourcePort="out0",
                                              target="todo", targetPort=f"p{i}"))
    state = Component(id="todo", component="ToDo", metadata=SqlNodeMetadata(label="ToDo_1"),
                      ports=NodePorts(inputs=inputs, outputs=[]), properties=props("m", relations=()))
    context = SqlContext(graph=SqlGraph(connections=connections, nodes=nodes), projectName="p",
                         projectMacros=[], dependencyProjectMacros={})
    return ToDo().onChange(context, state, state).properties


def test_each_input_is_named_as_the_cte_it_reads_in_port_order():
    p = _onchange([("in0", "Join_26_inner"), ("Filter_23_out1", "Filter_23"), ("in2", "Select_5")])
    assert p.relation_name == ["Join_26_inner", "Filter_23_out1", "Select_5"]


def test_an_unconnected_input_is_left_out_of_the_call():
    p = _onchange([("in0", "Join_26_inner"), ("in1", None)])
    assert p.relation_name == ["Join_26_inner", ""]
    assert source_properties(ToDo().apply(p)).parameters[1].value == "['Join_26_inner']"


def test_several_inputs_survive_save_cycles():
    gem = ToDo()
    p = props("m", relations=("Join_26_inner", "Filter_23_out1", "Select_5"), error=None, code=None)
    first = gem.apply(p)
    for _ in range(5):
        p = gem.loadProperties(source_properties(gem.apply(p)))
    assert p.relation_name == ["Join_26_inner", "Filter_23_out1", "Select_5"] and gem.apply(p) == first
    assert rendered_args(first) == ("m", ["Join_26_inner", "Filter_23_out1", "Select_5"])
