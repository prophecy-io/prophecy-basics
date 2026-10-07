"""Tests for code_to_visual_runner.py.

Unit tests of the runner's own logic; they need only jinja2 + pyyaml. The
end-to-end round trip over app_generated/ is run by CI through the runner CLI.
"""

import dataclasses
import os
import sys
import typing
from typing import Dict, List, Optional

import pytest
from jinja2 import nodes

sys.path.insert(0, os.path.dirname(__file__))
import code_to_visual_runner as c2v  # noqa: E402  (module import: its test_file is not a pytest test)

ENV = c2v._make_env()


def expr(text):
    return c2v._parse_expr(ENV, text)


def call(text):
    return next(ENV.parse("{{ " + text + " }}").find_all(nodes.Call))


# ---- generate_code ----------------------------------------------------------

@pytest.mark.parametrize("src", [
    "'a'", "1", "-2.5", "true", "none", "[1, 'x']", "(1,)", "{'k': [1, 2]}",
    "x.y[0]", "a ~ b", "a if b else c", "a in b", "a not in b", "x | f(1)",
    "ref('t')", "not a", "a and b or c", "1 + 2",
])
def test_generate_code_round_trips(src):
    assert c2v.generate_code(expr(src)) == src


def test_generate_code_escapes_quotes_and_warns():
    warnings = []
    out = c2v.generate_code(nodes.Const("it's"), warnings)
    assert out == "'it\\'s'"
    assert warnings and "apostrophe" in warnings[0]


def test_generate_code_rejects_unknown_nodes():
    with pytest.raises(c2v.GenerateCodeError):
        c2v.generate_code(nodes.Slice(None, None, None))


# ---- literal_value / values_equal -------------------------------------------

def test_literal_value_only_accepts_pure_literals():
    assert c2v.literal_value(expr("['a', {'k': -1}, true]")) == ["a", {"k": -1}, True]
    for src in ["x", "ref('t')", "1 + 1", "'a' ~ 'b'", "-true"]:
        assert c2v.literal_value(expr(src)) is c2v._NON_LITERAL, src


@pytest.mark.parametrize("a, b, equal", [
    ("['a','b']", "['a', 'b']", True),
    ('"x"', "'x'", True),
    ("1", "'1'", False),
    ("true", "1", False),
    ("1", "1.0", False),
    ("['a']", "['a', 'b']", False),
    ("1 + 1", "2", False),          # expressions are not folded
    ("ref('t')", " ref('t') ", True),
])
def test_values_equal(a, b, equal):
    assert c2v.values_equal(ENV, a, b) is equal


# ---- static shape check ------------------------------------------------------

@pytest.mark.parametrize("src, ann, error", [
    ("['t']", List[str], False),
    ("'t'", List[str], True),
    ("{'a': 1}", List[str], True),
    ("x", List[str], False),        # unknowable statically
    ("true", bool, False),
    ("'true'", bool, True),
    ("'4'", int, False),            # numeric strings are accepted
    ("'four'", int, True),
    ("[1]", Optional[str], False),  # str fields accept anything
])
def test_check_value_shape(src, ann, error):
    assert (c2v.check_value_shape(expr(src), ann) is not None) is error


# ---- recursive typecheck -----------------------------------------------------

@dataclasses.dataclass
class Inner:
    n: int


@dataclasses.dataclass
class Outer:
    items: List[Inner]
    label: Optional[str] = None
    flags: Dict[str, bool] = dataclasses.field(default_factory=dict)


def test_typecheck_reports_every_violation_with_path():
    violations = []
    c2v._typecheck_dataclass(Outer([Inner(1), Inner(True)], 3, {"a": 1}), "", violations)
    assert violations == [
        "items[1].n: expected int, got bool",
        "label: expected str | None, got int",
        "flags['a']: expected bool, got int",
    ]


def test_typecheck_accepts_valid_values():
    violations = []
    c2v._typecheck_dataclass(Outer([Inner(1)], None, {"a": True}), "", violations)
    assert violations == []


# ---- serialization-boundary coercion ----------------------------------------

@dataclasses.dataclass
class Col:
    expression: str
    format: Optional[str] = None


@dataclasses.dataclass
class Rule:
    expression: Col
    sortType: str


MOD = __name__


def coerce(value, ann):
    return c2v._coerce_to_annotation(value, ann, MOD)


def test_coerce_rebuilds_nested_gem_dataclasses():
    payload = [{"expression": {"expression": "age"}, "sortType": "asc", "extra": 1}]
    assert coerce(payload, List[Rule]) == [Rule(Col("age"), "asc")]
    assert coerce({"expression": "a"}, Optional[Col]) == Col("a")


def test_coerce_keeps_none_and_retypes_whole_floats():
    assert coerce(None, Optional[Col]) is None
    assert type(coerce(2.0, int)) is int
    assert coerce(2.5, int) == 2.5
    assert coerce([1.0, {"k": 3.0}], list) == [1, {"k": 3}]
    assert coerce((1.0, "x"), typing.Tuple[float, ...]) == (1, "x")


def test_coerce_missing_required_arg_names_the_path():
    with pytest.raises(c2v.BoundaryRehydrationError, match=r"'\[0\]\.expression'"):
        coerce([{"expression": {"format": "x"}, "sortType": "asc"}], List[Rule])


def test_coerce_skips_dataclasses_from_other_modules():
    assert coerce({"n": 1}, c2v.MacroSignature) == {"n": 1}


# ---- resolution + validation -------------------------------------------------

SIG = c2v.MacroSignature("Gem", ["relation_name", "cols", "limit"], {"limit": "10"})


@dataclasses.dataclass
class GemProps:
    relation_name: List[str]
    cols: List[str]
    limit: int = 10


GEM = c2v.GemInfo(instance=None, properties_cls=GemProps, gem_name="Gem",
                  project_name="proj")


def validate(src):
    c = call(src)
    params = c2v.resolve_params(c, SIG, [])
    return params, c2v.validate_params(c, SIG, params, GEM, ENV)


def test_resolve_names_positionals_from_signature():
    params, errors = validate("proj.Gem(['t'], cols=['a'])")
    assert [(p.name, p.value, p.origin) for p in params] == [
        ("relation_name", "['t']", "pos"), ("cols", "['a']", "kw")]
    assert errors == []


@pytest.mark.parametrize("src, fragment", [
    ("proj.Gem(['t'])", "'cols' is missing"),
    ("proj.Gem(['t'], ['a'], 1, 2)", "overflows"),
    ("proj.Gem(['t'], ['a'], cols=['b'])", "supplied 2 times"),
    ("proj.Gem(['t'], ['a'], nope=1)", "unknown keyword"),
    ("proj.Gem('t', ['a'])", "expected a list"),
    ("proj.Gem(*x)", "*args"),
])
def test_validate_rejects(src, fragment):
    _, errors = validate(src)
    assert any(fragment in e for e in errors), errors


# ---- extraction ------------------------------------------------------------

def test_scan_brace_spans_handles_nesting_and_quotes():
    src = "a {{ f({'k': '}}'}) }}\n{{ g() }}"
    assert c2v._scan_brace_spans(src) == [(1, " f({'k': '}}'}) "), (2, " g() ")]


def test_extract_calls_falls_back_and_skips_comments(tmp_path):
    project = c2v.Project(str(tmp_path), "proj", {("proj", "Gem"): GEM}, {})
    sql = tmp_path / "m.sql"
    sql.write_text(
        "{% snapshot s %}\n"                      # dbt-only tag: whole-file parse fails
        "{# {{ proj.Gem(['c']) }} #}\n"
        "{{ proj.Gem(['t'], ['a']) }}\n"
        "{{ Gem(['u'], ['b']) }}\n"
        "{{ other.Gem(['v'], ['c']) }}\n"
        "{% endsnapshot %}\n")
    warnings = []
    calls = c2v.extract_calls(str(sql), project, warnings)
    assert [(c.fq, c.lineno) for c in calls] == [("proj.Gem", 3), ("proj.Gem", 4)]
    assert "falling back" in warnings[0]


def test_expand_paths(tmp_path):
    (tmp_path / "b.sql").write_text("")
    (tmp_path / "a.sql").write_text("")
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "c.sql").write_text("")
    (tmp_path / "empty").mkdir()
    files, errors = c2v.expand_paths([str(tmp_path), str(tmp_path / "empty"),
                                      str(tmp_path / "missing")])
    assert [os.path.basename(f) for f in files] == ["a.sql", "b.sql"]
    assert len(errors) == 2

