"""ToDo's serializer and loader against what the SQL Editor actually hands them.

A transpiled ToDo lost three things on every code -> visual cycle: its message gained a pair of
quotes each time (ToDo(''msg'') then no longer compiles), its "Error message" and "Helper
code/text" were dropped, and -- because the call named no input -- the editor rebuilt the gem
with no input and moved the CTEs before it into a model of their own, losing the edge.

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

# The dbt macro signature, in order: the SQL Editor names positional arguments from it.
ARGUMENT_NAMES = ("diag_message", "relation_name", "error_string", "code_string")
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
    assert args == (message, ["AlteryxSelect_28"], "Report Text has no SQL form", CODE)


@pytest.mark.parametrize("message", MESSAGES)
def test_code_visual_save_cycles_change_nothing(message):
    gem, p = ToDo(), props(message)
    first = gem.apply(p)
    for _ in range(5):
        p = gem.loadProperties(source_properties(gem.apply(p)))
        assert p == props(message)
    assert gem.apply(p) == first  # no quotes or escapes added per cycle


def test_the_call_names_the_gems_inputs():
    """The SQL Editor takes a macro gem's inputs from its relation_name argument; without it
    the gem comes back with no input and the CTEs feeding it move to a model of their own."""
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


def test_an_unloaded_value_that_is_itself_a_jinja_literal_is_decoded():
    """The one ambiguity of a raw value: it cannot be told from the code's quoted token."""
    p = ToDo().loadProperties(ToDo().unloadProperties(props("'already quoted'")))
    assert p.diag_message == "already quoted"


def test_no_error_or_helper_code_keeps_the_call_short():
    call = ToDo().apply(props("m", error=None, code=None))
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
    ("ToDo(\"m\", ['AlteryxSelect_28'], \"err\", \"<Node/>\")", "m"),
])
def test_the_dbt_macro_accepts_old_and_new_calls(call, message):
    env, sql = _macro_env()
    out = env.from_string(sql + "{{ " + call + " }}").render().strip()
    assert out == f"<{message}>"  # only the message reaches the adapter's SQL
