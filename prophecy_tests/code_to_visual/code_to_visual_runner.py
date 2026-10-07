#!/usr/bin/env python3
"""
code_to_visual_runner.py -- round-trip tester for Prophecy SQL macro gems.

Finds gem macro calls in a dbt/jinja SQL file, such as::

    {{ prophecy_basics.FindDuplicates('src', ['id','name'], ...) }}

and checks that each call survives the code -> visual -> code cycle the SQL
Editor performs when it opens a project:

    extract   -> find calls to registered gems.
    resolve   -> name each argument from the dbt macro signature (raw source text).
    validate  -> reject malformed calls.
    load      -> run the gem's loadProperties.
    rehydrate -> emulate the Sandbox serialization round trip.
    onChange  -> emulate schema analysis.
    typecheck -> verify the properties against their annotations.
    apply     -> run the gem's apply to regenerate the call.
    compare   -> reparse the regenerated call and diff it against original.

Argument values are kept as raw source text; decoding them is each gem's job.
Parameters absent from the call are not pre-filled, so a gem reading one gets
None, exactly as in the SQL Editor.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import importlib.util
import inspect
import io
import os
import re
import sys
import traceback
import typing
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import jinja2
import yaml
from jinja2 import nodes


# =============================================================================
# jinja environment
# =============================================================================

def _make_env() -> jinja2.Environment:
    # dbt-only tags (e.g. materialization) are handled by the callers.
    return jinja2.Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])


# =============================================================================
# Code generation: jinja expression node -> source text
# =============================================================================

class GenerateCodeError(Exception):
    """Raised when generate_code meets a node type it does not know how to render."""

    def __init__(self, node_type: str, lineno: Optional[int]):
        self.node_type = node_type
        self.lineno = lineno
        super().__init__(f"cannot render jinja node {node_type} (line {lineno})")


# Comparison operator symbols for nodes.Compare operands.
_CMP_OPS = {
    "eq": "==", "ne": "!=", "lt": "<", "lteq": "<=",
    "gt": ">", "gteq": ">=", "in": " in ", "notin": " not in ",
}


def generate_code(node: nodes.Node, warnings: Optional[List[str]] = None) -> str:
    """Deterministically render a jinja2 expression node to source text.

    Strings are single quoted, booleans render as true/false, None as none.
    Unknown node types raise GenerateCodeError.
    """
    g = lambda n: generate_code(n, warnings)

    if isinstance(node, nodes.Const):
        v = node.value
        if isinstance(v, bool):
            return "true" if v else "false"
        if v is None:
            return "none"
        if isinstance(v, str):
            if "'" in v and warnings is not None:
                warnings.append(
                    "value contains an apostrophe: gems that decode via "
                    "replace(\"'\", '\"') will mis-parse "
                    f"{v!r}"
                )
            return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
        return repr(v)  # int / float -> bare

    if isinstance(node, nodes.Name):
        return node.name

    if isinstance(node, nodes.Getattr):
        return f"{g(node.node)}.{node.attr}"

    if isinstance(node, nodes.Getitem):
        return f"{g(node.node)}[{g(node.arg)}]"

    if isinstance(node, nodes.List):
        return "[" + ", ".join(g(i) for i in node.items) + "]"

    if isinstance(node, nodes.Tuple):
        if len(node.items) == 1:
            return f"({g(node.items[0])},)"
        inner = ", ".join(g(i) for i in node.items)
        return f"({inner})"

    if isinstance(node, nodes.Dict):
        pairs = ", ".join(f"{g(p.key)}: {g(p.value)}" for p in node.items)
        return "{" + pairs + "}"

    if isinstance(node, nodes.Neg):
        return f"-{g(node.node)}"
    if isinstance(node, nodes.Pos):
        return f"+{g(node.node)}"
    if isinstance(node, nodes.Not):
        return f"not {g(node.node)}"

    if isinstance(node, nodes.Concat):
        return " ~ ".join(g(n) for n in node.nodes)

    if isinstance(node, nodes.CondExpr):
        expr2 = f" else {g(node.expr2)}" if node.expr2 is not None else ""
        return f"{g(node.expr1)} if {g(node.test)}{expr2}"

    if isinstance(node, nodes.Compare):
        out = g(node.expr)
        for op in node.ops:
            sym = _CMP_OPS.get(op.op, " ? ")
            if sym.startswith(" "):
                out += f"{sym}{g(op.expr)}"  # e.g. " in x"
            else:
                out += f" {sym} {g(op.expr)}"
        return out

    if isinstance(node, nodes.And):
        return f"{g(node.left)} and {g(node.right)}"
    if isinstance(node, nodes.Or):
        return f"{g(node.left)} or {g(node.right)}"

    if isinstance(node, nodes.BinExpr):
        return f"{g(node.left)} {node.operator} {g(node.right)}"

    if isinstance(node, nodes.Filter):
        base = g(node.node) if node.node is not None else ""
        if node.args or node.kwargs:
            return f"{base} | {node.name}{_render_call_args(node, g)}"
        return f"{base} | {node.name}"

    if isinstance(node, nodes.Call):
        return f"{g(node.node)}{_render_call_args(node, g)}"

    raise GenerateCodeError(type(node).__name__, getattr(node, "lineno", None))


def _render_call_args(node, g) -> str:
    parts = [g(a) for a in node.args]
    parts += [f"{k.key}={g(k.value)}" for k in node.kwargs]
    return "(" + ", ".join(parts) + ")"


# =============================================================================
# Literal extraction and type-sensitive comparison
# =============================================================================

_NON_LITERAL = object()  # sentinel: node is not a pure literal tree


def literal_value(node: nodes.Node) -> Any:
    """Return the python value of a pure-literal node tree, else _NON_LITERAL."""
    if isinstance(node, nodes.Const):
        return node.value
    if isinstance(node, nodes.List):
        vals = [literal_value(i) for i in node.items]
        return _NON_LITERAL if any(v is _NON_LITERAL for v in vals) else vals
    if isinstance(node, nodes.Tuple):
        vals = [literal_value(i) for i in node.items]
        return _NON_LITERAL if any(v is _NON_LITERAL for v in vals) else tuple(vals)
    if isinstance(node, nodes.Dict):
        out = {}
        for p in node.items:
            k = literal_value(p.key)
            v = literal_value(p.value)
            if k is _NON_LITERAL or v is _NON_LITERAL:
                return _NON_LITERAL
            try:
                out[k] = v
            except TypeError:
                return _NON_LITERAL
        return out
    if isinstance(node, nodes.Neg):
        inner = literal_value(node.node)
        return -inner if isinstance(inner, (int, float)) and not isinstance(inner, bool) else _NON_LITERAL
    if isinstance(node, nodes.Pos):
        inner = literal_value(node.node)
        return +inner if isinstance(inner, (int, float)) and not isinstance(inner, bool) else _NON_LITERAL
    return _NON_LITERAL


def _parse_expr(env: jinja2.Environment, text: str) -> Optional[nodes.Node]:
    """Parse ``{{ text }}`` and return the single output expression node, or None."""
    try:
        ast = env.parse("{{ " + text + " }}")
    except jinja2.exceptions.TemplateSyntaxError:
        return None
    for out in ast.find_all(nodes.Output):
        if out.nodes:
            return out.nodes[0]
    return None


def strict_equal(a: Any, b: Any) -> bool:
    """Type-sensitive equality: 1 vs '1' differ, True vs 1 differ, 1 vs 1.0 differ."""
    if type(a) is not type(b):
        return False
    if isinstance(a, list):
        return len(a) == len(b) and all(strict_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, tuple):
        return len(a) == len(b) and all(strict_equal(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(strict_equal(a[k], b[k]) for k in a)
    return a == b


def values_equal(env: jinja2.Environment, text_a: str, text_b: str) -> bool:
    """Compare two raw value texts: literal trees compare by value, else by text."""
    if text_a == text_b:
        return True
    na = _parse_expr(env, text_a)
    nb = _parse_expr(env, text_b)
    if na is None or nb is None:
        return text_a.strip() == text_b.strip()
    la = literal_value(na)
    lb = literal_value(nb)
    if la is _NON_LITERAL or lb is _NON_LITERAL:
        return text_a.strip() == text_b.strip()
    return strict_equal(la, lb)


# =============================================================================
# Type-shape category (advisory check + typecheck helpers)
# =============================================================================

def _field_types(cls) -> Dict[str, Any]:
    """Resolved field annotations of a dataclass; raw ones if resolution fails."""
    try:
        return typing.get_type_hints(cls)
    except Exception:
        return {f.name: f.type for f in dataclasses.fields(cls)}


def _strip_optional(ann):
    """Return the list of Union members with NoneType removed (or [ann])."""
    origin = typing.get_origin(ann)
    if origin is typing.Union:
        return [a for a in typing.get_args(ann) if a is not type(None)]
    return [ann]


def shape_category(ann) -> str:
    """Coarse category for the advisory shape check: list/bool/numeric/str/other."""
    members = _strip_optional(ann)
    cats = set()
    for m in members:
        origin = typing.get_origin(m)
        if origin in (list, tuple) or m in (list, tuple):
            cats.add("list")
        elif m is bool:
            cats.add("bool")
        elif m in (int, float):
            cats.add("numeric")
        elif m is str:
            cats.add("str")
        else:
            cats.add("other")
    # str is the most tolerant; prefer it if present so we never false-fail.
    if "str" in cats:
        return "str"
    if cats == {"list"}:
        return "list"
    if cats == {"bool"}:
        return "bool"
    if cats == {"numeric"}:
        return "numeric"
    return "other"


def _is_numeric_string(node: nodes.Const) -> bool:
    if not isinstance(node.value, str):
        return False
    try:
        float(node.value)
        return True
    except ValueError:
        return False


def check_value_shape(value_node: nodes.Node, ann) -> Optional[str]:
    """Static shape check of an argument node against a field annotation.

    Returns an error message on a clear mismatch, else None. A scalar in a
    List field is always an error: the SQL Editor emits list literals for them.
    """
    cat = shape_category(ann)
    if cat == "str" or cat == "other":
        return None  # string fields accept anything; complex types -> typecheck

    if cat == "list":
        if isinstance(value_node, (nodes.List, nodes.Tuple)):
            return None
        if isinstance(value_node, nodes.Const):
            return f"expected a list, got scalar {type(value_node.value).__name__}"
        if isinstance(value_node, nodes.Dict):
            return "expected a list, got a dict literal"
        return None  # Name / Call / etc. -> cannot tell statically

    if cat == "bool":
        if isinstance(value_node, nodes.Const):
            if isinstance(value_node.value, bool):
                return None
            return f"expected a boolean, got {type(value_node.value).__name__} literal"
        if isinstance(value_node, (nodes.List, nodes.Tuple, nodes.Dict)):
            return "expected a boolean, got a collection literal"
        return None

    if cat == "numeric":
        if isinstance(value_node, nodes.Const):
            v = value_node.value
            if isinstance(v, bool):
                return "expected a number, got a boolean literal"
            if isinstance(v, (int, float)):
                return None
            if _is_numeric_string(value_node):
                return None
            return f"expected a number, got {type(v).__name__} literal {v!r}"
        if isinstance(value_node, (nodes.List, nodes.Tuple, nodes.Dict)):
            return "expected a number, got a collection literal"
        return None

    return None


# =============================================================================
# Recursive dataclass typecheck
# =============================================================================

def _type_name(ann) -> str:
    if ann is type(None):
        return "None"
    if isinstance(ann, type):
        return ann.__name__
    return str(ann).replace("typing.", "")


def value_satisfies(value: Any, ann, path: str, violations: List[str]) -> None:
    """Recursively verify ``value`` against annotation ``ann``.

    Appends ``"<path>: expected <T>, got <U>"`` for each violation.
    bool satisfies bool only; int satisfies int/float; float satisfies float.
    """
    if ann is Any or ann is None or ann is object:
        return

    origin = typing.get_origin(ann)

    # Optional / Union
    if origin is typing.Union:
        members = typing.get_args(ann)
        if value is None and type(None) in members:
            return
        # pass if any non-None member matches with no violations
        for m in _strip_optional(ann):
            sub: List[str] = []
            value_satisfies(value, m, path, sub)
            if not sub:
                return
        opts = " | ".join(_type_name(m) for m in members)
        violations.append(f"{path}: expected {opts}, got {_type_name(type(value))}")
        return

    # List / Sequence
    if origin in (list, typing.List) or ann is list:
        if not isinstance(value, list):
            violations.append(f"{path}: expected list, got {_type_name(type(value))}")
            return
        args = typing.get_args(ann)
        if args:
            for i, item in enumerate(value):
                value_satisfies(item, args[0], f"{path}[{i}]", violations)
        return

    if origin in (tuple,) or ann is tuple:
        if not isinstance(value, tuple):
            violations.append(f"{path}: expected tuple, got {_type_name(type(value))}")
        return

    # Dict / Mapping
    if origin in (dict,) or ann is dict:
        if not isinstance(value, dict):
            violations.append(f"{path}: expected dict, got {_type_name(type(value))}")
            return
        args = typing.get_args(ann)
        if len(args) == 2:
            for k, v in value.items():
                value_satisfies(k, args[0], f"{path}.<key>", violations)
                value_satisfies(v, args[1], f"{path}[{k!r}]", violations)
        return

    # plain classes
    if isinstance(ann, type):
        if ann is bool:
            if not isinstance(value, bool):
                violations.append(f"{path}: expected bool, got {_type_name(type(value))}")
            return
        if ann is int:
            if isinstance(value, bool) or not isinstance(value, int):
                violations.append(f"{path}: expected int, got {_type_name(type(value))}")
            return
        if ann is float:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                violations.append(f"{path}: expected float, got {_type_name(type(value))}")
            return
        if ann is str:
            if not isinstance(value, str):
                violations.append(f"{path}: expected str, got {_type_name(type(value))}")
            return
        if dataclasses.is_dataclass(ann):
            if not isinstance(value, ann):
                violations.append(f"{path}: expected {ann.__name__}, got {_type_name(type(value))}")
                return
            _typecheck_dataclass(value, path, violations)
            return
        # any other concrete class
        if not isinstance(value, ann):
            violations.append(f"{path}: expected {ann.__name__}, got {_type_name(type(value))}")
        return

    # Unresolved forward reference: report it rather than silently pass.
    if isinstance(ann, str):
        violations.append(
            f"{path}: unresolved type annotation {ann!r} — typecheck could not "
            f"verify this field")
        return

    # unknown typing construct -> skip
    return


def _typecheck_dataclass(obj: Any, path: str, violations: List[str]) -> None:
    hints = _field_types(type(obj))
    for f in dataclasses.fields(obj):
        ann = hints.get(f.name, f.type)
        val = getattr(obj, f.name)
        child = f"{path}.{f.name}" if path else f.name
        value_satisfies(val, ann, child, violations)


# =============================================================================
# Discovery: project name, macro signatures, gem registry
# =============================================================================

@dataclass
class MacroSignature:
    name: str
    args: List[str]  # ordered parameter names
    defaults: Dict[str, str]  # param name -> default rendered as text

    @property
    def arity(self) -> int:
        return len(self.args)


@dataclass
class GemInfo:
    instance: Any  # MacroSpec subclass instance
    properties_cls: Optional[type]  # nested MacroProperties dataclass (or None)
    gem_name: str
    project_name: str


@dataclass
class Project:
    project_dir: str
    project_name: str
    gems: Dict[Tuple[str, str], GemInfo]  # (projectName, gemName) -> GemInfo
    signatures: Dict[str, MacroSignature]  # gemName -> signature
    warnings: List[str] = field(default_factory=list)
    # Gems that fail to import/instantiate; these fail the run.
    errors: List[str] = field(default_factory=list)
    failed_gems: Dict[str, str] = field(default_factory=dict)  # name/stem -> error


_PROJECT_CACHE: Dict[str, Project] = {}


def find_project_dir(start: str) -> str:
    """Walk up from ``start`` until a dbt_project.yml is found."""
    cur = os.path.abspath(start)
    if os.path.isfile(cur):
        cur = os.path.dirname(cur)
    while True:
        if os.path.isfile(os.path.join(cur, "dbt_project.yml")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            raise FileNotFoundError(
                f"no dbt_project.yml found walking up from {start!r}"
            )
        cur = parent


def _load_signatures(project_dir: str, macro_paths: List[str],
                     gem_names: set, warnings: List[str]) -> Dict[str, MacroSignature]:
    env = _make_env()
    sigs: Dict[str, MacroSignature] = {}
    for mp in macro_paths:
        mdir = os.path.join(project_dir, mp)
        if not os.path.isdir(mdir):
            continue
        for fn in sorted(os.listdir(mdir)):
            if not fn.endswith(".sql"):
                continue
            path = os.path.join(mdir, fn)
            src = open(path, encoding="utf-8").read()
            # plain jinja can't parse dbt materialization blocks; they hold no gem macro
            src = _blank_out(r"\{%-?\s*materialization\b.*?\{%-?\s*endmaterialization\s*-?%\}", src)
            try:
                ast = env.parse(src)
            except Exception as e:
                warnings.append(f"could not parse macro file {fn}: {type(e).__name__}: {e}")
                continue
            for m in ast.find_all(nodes.Macro):
                # Exact gem names only; adapter variants (databricks__X) are ignored.
                if m.name not in gem_names:
                    continue
                args = [a.name for a in m.args]
                # defaults align to the LAST len(defaults) args.
                defaults: Dict[str, str] = {}
                if m.defaults:
                    tail = args[len(args) - len(m.defaults):]
                    for name, dnode in zip(tail, m.defaults):
                        try:
                            defaults[name] = generate_code(dnode)
                        except GenerateCodeError:
                            defaults[name] = "<default>"
                sigs[m.name] = MacroSignature(m.name, args, defaults)
    return sigs


def _load_gems(project_dir: str, project_name: str, warnings: List[str],
               errors: List[str],
               failed_gems: Dict[str, str]) -> Dict[Tuple[str, str], GemInfo]:
    from prophecy.cb.sql.MacroBuilderBase import MacroSpec
    from prophecy.cb.sql.Component import MacroProperties

    gems_dir = os.path.join(project_dir, "gems")
    registry: Dict[Tuple[str, str], GemInfo] = {}
    if not os.path.isdir(gems_dir):
        warnings.append(f"no gems/ directory under {project_dir}")
        return registry
    if gems_dir not in sys.path:
        sys.path.insert(0, gems_dir)

    for fn in sorted(os.listdir(gems_dir)):
        if not fn.endswith(".py") or fn in ("__init__.py", "setup.py"):
            continue
        path = os.path.join(gems_dir, fn)
        mod_name = "prophecy_gem_" + fn[:-3]
        try:
            spec = importlib.util.spec_from_file_location(mod_name, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[mod_name] = module
            spec.loader.exec_module(module)
        except Exception as e:
            msg = f"could not import gem {fn}: {type(e).__name__}: {e}"
            errors.append(msg)
            failed_gems[fn[:-3]] = msg  # gem name is conventionally the file stem
            continue

        for obj_name, obj in vars(module).items():
            if not (inspect.isclass(obj) and issubclass(obj, MacroSpec) and obj is not MacroSpec):
                continue
            if obj.__module__ != mod_name:
                continue  # skip re-exported base/other classes
            try:
                instance = obj()
            except Exception as e:
                msg = f"could not instantiate gem {obj_name} in {fn}: {e}"
                errors.append(msg)
                failed_gems[obj_name] = msg
                continue
            props_cls = None
            for _, nested in vars(obj).items():
                if (inspect.isclass(nested) and issubclass(nested, MacroProperties)
                        and nested is not MacroProperties):
                    props_cls = nested
                    break
            gem_name = getattr(instance, "name", obj_name)
            proj = getattr(instance, "projectName", "") or project_name
            registry[(proj, gem_name)] = GemInfo(
                instance=instance, properties_cls=props_cls,
                gem_name=gem_name, project_name=proj,
            )
    return registry


def load_project(project_dir: str) -> Project:
    """Discover project name, macro signatures and the gem registry (cached)."""
    project_dir = os.path.abspath(project_dir)
    if project_dir in _PROJECT_CACHE:
        return _PROJECT_CACHE[project_dir]

    dbt_yml = os.path.join(project_dir, "dbt_project.yml")
    if not os.path.isfile(dbt_yml):
        raise FileNotFoundError(f"no dbt_project.yml in {project_dir}")
    with open(dbt_yml, encoding="utf-8") as fh:
        conf = yaml.safe_load(fh) or {}
    project_name = str(conf.get("name"))
    if not project_name:
        raise ValueError(f"dbt_project.yml in {project_dir} has no 'name'")
    macro_paths = conf.get("macro-paths") or ["macros"]

    warnings: List[str] = []
    errors: List[str] = []
    failed_gems: Dict[str, str] = {}
    gems = _load_gems(project_dir, project_name, warnings, errors, failed_gems)
    gem_names = {g.gem_name for g in gems.values()}
    signatures = _load_signatures(project_dir, macro_paths, gem_names, warnings)

    project = Project(project_dir, project_name, gems, signatures, warnings,
                      errors, failed_gems)
    _PROJECT_CACHE[project_dir] = project
    return project


# =============================================================================
# Extraction of gem macro calls from a SQL file
# =============================================================================

@dataclass
class GemCall:
    node: nodes.Call
    project: str
    gem: str
    lineno: int

    @property
    def fq(self) -> str:
        return f"{self.project}.{self.gem}"


def _callee_names(call: nodes.Call, default_project: str) -> Optional[Tuple[str, str]]:
    """Return (project, gem) for a Call whose callee is Getattr(Name, attr) or Name."""
    callee = call.node
    if isinstance(callee, nodes.Getattr) and isinstance(callee.node, nodes.Name):
        return callee.node.name, callee.attr
    if isinstance(callee, nodes.Name):
        return default_project, callee.name
    return None


def _blank_out(pattern: str, src: str) -> str:
    """Replace every match with spaces, preserving newlines (keeps line numbers)."""
    return re.sub(pattern,
                  lambda m: re.sub(r"[^\n]", " ", m.group(0)),
                  src, flags=re.S)


def _scan_brace_spans(src: str) -> List[Tuple[int, str]]:
    """Quote-aware scan for balanced ``{{ ... }}`` spans -> (lineno, inner_text)."""
    spans = []
    i, n = 0, len(src)
    while i < n - 1:
        if src[i] == "{" and src[i + 1] == "{":
            j = i + 2
            depth = 0
            quote = None
            while j < n - 1:
                c = src[j]
                if quote:
                    if c == "\\":
                        j += 2
                        continue
                    if c == quote:
                        quote = None
                elif c in "'\"":
                    quote = c
                elif c in "([{":
                    depth += 1
                elif c == "}" and src[j + 1] == "}" and depth <= 0:
                    # span close: must be checked BEFORE the bracket-close branch,
                    # otherwise '}' is consumed as a dict/paren decrement.
                    inner = src[i + 2:j]
                    lineno = src.count("\n", 0, i) + 1
                    spans.append((lineno, inner))
                    i = j + 2
                    break
                elif c in ")]}":
                    depth -= 1
                j += 1
            else:
                break
            continue
        i += 1
    return spans


def extract_calls(path: str, project: Project, warnings: List[str]) -> List[GemCall]:
    env = _make_env()
    src = open(path, encoding="utf-8").read()
    default_proj = project.project_name
    registry = project.gems
    calls: List[GemCall] = []

    def collect_from_ast(ast, lineno_override=None):
        for call in ast.find_all(nodes.Call):
            names = _callee_names(call, default_proj)
            if names is None:
                continue
            proj, gem = names
            # Include gems that failed to load so they surface as failures.
            if (proj, gem) in registry or \
                    (proj == default_proj and gem in project.failed_gems):
                calls.append(GemCall(
                    call, proj, gem,
                    lineno_override or getattr(call, "lineno", 0) or 0))

    try:
        collect_from_ast(env.parse(src))
        return calls
    except jinja2.exceptions.TemplateSyntaxError as e:
        warnings.append(
            f"whole-file jinja parse failed ({e.message} @ line {e.lineno}); "
            "falling back to balanced-brace span scan"
        )

    # Fallback: parse each {{ ... }} span on its own, skipping comments and
    # raw blocks.
    src_scan = _blank_out(r"\{#.*?#\}", src)
    src_scan = _blank_out(r"\{%-?\s*raw\s*-?%\}.*?\{%-?\s*endraw\s*-?%\}", src_scan)
    for lineno, inner in _scan_brace_spans(src_scan):
        try:
            ast = env.parse("{{ " + inner + " }}")
        except jinja2.exceptions.TemplateSyntaxError:
            continue
        collect_from_ast(ast, lineno_override=lineno)
    return calls


# =============================================================================
# Resolution + validation
# =============================================================================

@dataclass
class ResolvedParam:
    name: str  # "" for overflow positionals
    value: str  # raw source text
    origin: str  # 'kw' | 'pos'
    index: int  # positional index (or -1 for kw)


def resolve_params(call: nodes.Call, sig: Optional[MacroSignature],
                   warnings: List[str]) -> List[ResolvedParam]:
    """Name positional args from the signature; keywords keep their own name."""
    params: List[ResolvedParam] = []
    for idx, arg in enumerate(call.args):
        value = generate_code(arg, warnings)
        if sig is not None and idx < sig.arity:
            params.append(ResolvedParam(sig.args[idx], value, "pos", idx))
        else:
            params.append(ResolvedParam("", value, "pos", idx))
    for kw in call.kwargs:
        value = generate_code(kw.value, warnings)
        params.append(ResolvedParam(kw.key, value, "kw", -1))
    return params


def validate_params(call: nodes.Call, sig: Optional[MacroSignature],
                    params: List[ResolvedParam], gem: GemInfo,
                    env: jinja2.Environment) -> List[str]:
    """Collect all validation errors for a call."""
    errors: List[str] = []

    if call.dyn_args is not None:
        errors.append("unsupported *args (dynamic positional splat) in macro call")
    if call.dyn_kwargs is not None:
        errors.append("unsupported **kwargs (dynamic keyword splat) in macro call")

    if sig is None:
        errors.append(
            f"cannot resolve positional arguments: no dbt macro signature found "
            f"for gem {gem.gem_name!r}"
        )

    # overflow positionals -> empty-name params
    for p in params:
        if p.origin == "pos" and p.name == "" and sig is not None:
            errors.append(
                f"positional argument #{p.index} (value {p.value}) overflows the "
                f"macro signature arity ({sig.arity})"
            )

    # duplicate param names (e.g. a kwarg colliding with a positional's sig name)
    seen: Dict[str, int] = {}
    for p in params:
        if p.name == "":
            continue
        seen[p.name] = seen.get(p.name, 0) + 1
    for name, cnt in seen.items():
        if cnt > 1:
            errors.append(f"parameter {name!r} is supplied {cnt} times")

    if sig is not None:
        sig_set = set(sig.args)
        # unknown keyword argument
        for p in params:
            if p.origin == "kw" and p.name not in sig_set:
                errors.append(
                    f"unknown keyword argument {p.name!r}: not a parameter of "
                    f"macro {sig.name}"
                )
        # missing required params (no default AND not supplied with a value)
        supplied = {p.name for p in params if p.name != "" and p.value != ""}
        for name in sig.args:
            if name not in supplied and name not in sig.defaults:
                errors.append(
                    f"required parameter {name!r} is missing and has no default"
                )

    # each supplied value must reparse as a valid jinja expression
    for p in params:
        if p.value == "":
            continue
        try:
            env.parse("{{ " + p.value + " }}")
        except jinja2.exceptions.TemplateSyntaxError:
            errors.append(f"value for {p.name or '<positional>'} does not reparse "
                          f"as a jinja expression: {p.value}")

    # static shape check against the gem's Properties dataclass
    if gem.properties_cls is not None:
        hints = _field_types(gem.properties_cls)
        # map resolved params back to their AST nodes
        arg_nodes = list(call.args) + [kw.value for kw in call.kwargs]
        for p, node in zip(params, arg_nodes):
            if p.name and p.name in hints:
                msg = check_value_shape(node, hints[p.name])
                if msg:
                    errors.append(f"parameter {p.name!r}: {msg}")

    return errors


# =============================================================================
# Stage "rehydrate": Sandbox serialization round trip
# =============================================================================

def _is_gem_module_level_dataclass(ann, gem_module: str) -> bool:
    """True iff ``ann`` is a dataclass defined at module level in the gem module.

    Only these are rebuilt into instances by the Sandbox; dataclasses nested
    inside the gem class stay plain dicts.
    """
    return (isinstance(ann, type)
            and dataclasses.is_dataclass(ann)
            and getattr(ann, "__module__", None) == gem_module
            and "." not in getattr(ann, "__qualname__", "."))


class BoundaryRehydrationError(Exception):
    """A nested dataclass could not be rebuilt at the serialization boundary."""


def _src_location(cls) -> str:
    try:
        return f"{inspect.getsourcefile(cls)}:{inspect.getsourcelines(cls)[1]}"
    except Exception:
        return getattr(cls, "__module__", "?")


def _construct_dataclass(datacls, payload: dict, gem_module: str,
                                    path: str = ""):
    """Rebuild ``datacls`` from ``payload`` the way the Sandbox does.

    Unknown keys are dropped; a missing required constructor arg raises
    BoundaryRehydrationError. Nested values are coerced first.
    """
    hints = _field_types(datacls)
    ctor_params = inspect.signature(datacls.__init__).parameters  # includes 'self'
    kwargs = {}
    for key, value in payload.items():
        if key == "self" or key not in ctor_params:
            continue
        kwargs[key] = _coerce_to_annotation(value, hints.get(key, Any), gem_module,
                                            path=f"{path}.{key}" if path else key)

    missing = [n for n, p in ctor_params.items()
               if n != "self" and p.default is inspect.Parameter.empty
               and n not in kwargs]
    if missing:
        cls = datacls.__name__
        raise BoundaryRehydrationError(
            f"cannot rebuild nested dataclass '{cls}' at property path "
            f"'{path or '<root>'}'\n"
            f"  value from the macro call : {payload!r}\n"
            f"  missing required ctor args: {', '.join(missing)}  "
            f"(class {cls} at {_src_location(datacls)} declares "
            f"{'them' if len(missing) > 1 else 'it'} with no default)\n"
            f"  The SQL Editor fails the same way: code generation for this gem "
            f"breaks when the project is reopened.\n"
            f"  Fix the gem (either one):\n"
            f"    - give the field a default in class {cls}, e.g. "
            f"`{missing[0]}: Optional[str] = None`\n"
            f"    - or make apply() emit the "
            f"{'keys' if len(missing) > 1 else 'key'} "
            f"({', '.join(repr(m) for m in missing)}) in the dicts it generates,"
            f" so the macro call carries "
            f"{'them' if len(missing) > 1 else 'it'}"
        )
    return datacls(**kwargs)


def _coerce_to_annotation(value: Any, ann, gem_module: str, path: str = "") -> Any:
    """Recursively coerce ``value`` toward ``ann`` at the serialization boundary.

    Only plain dicts targeting a gem-module-level dataclass are rebuilt; all
    other values pass through structurally unchanged.
    """
    if value is None:
        return None  # nulls are never replaced by defaults

    # Numbers are re-typed by value: whole floats come back as ints.
    if isinstance(value, float) and value.is_integer():
        return int(value)

    origin = typing.get_origin(ann)

    if origin is typing.Union:  # Optional[X] -> X; wider unions -> untyped
        members = _strip_optional(ann)
        ann = members[0] if len(members) == 1 else Any
        origin = typing.get_origin(ann)

    if _is_gem_module_level_dataclass(ann, gem_module):
        if isinstance(value, ann):
            return _recurse_dataclass_boundary(value, gem_module, path)
        if isinstance(value, dict):
            return _construct_dataclass(ann, value, gem_module, path)
        return value

    # Containers: coerce elements to the annotated item type, else untyped
    # (numbers inside are still re-typed).
    args = typing.get_args(ann)
    if isinstance(value, (list, tuple)):
        item = args[0] if origin in (list, tuple) and args else Any
        coerced = [_coerce_to_annotation(v, item, gem_module, f"{path}[{i}]")
                   for i, v in enumerate(value)]
        return tuple(coerced) if isinstance(value, tuple) else coerced
    if isinstance(value, dict):
        item = args[1] if origin is dict and len(args) == 2 else Any
        return {k: _coerce_to_annotation(v, item, gem_module, f"{path}[{k!r}]")
                for k, v in value.items()}

    return value


def _recurse_dataclass_boundary(obj: Any, gem_module: str, path: str = "") -> Any:
    """Coerce each field of an existing dataclass instance; rebuild if changed."""
    hints = _field_types(type(obj))
    changes = {}
    for f in dataclasses.fields(obj):
        cur = getattr(obj, f.name)
        new = _coerce_to_annotation(cur, hints.get(f.name, Any), gem_module,
                                    f"{path}.{f.name}" if path else f.name)
        if new is not cur:
            changes[f.name] = new
    if changes:
        return dataclasses.replace(obj, **changes)  # works for frozen dataclasses
    return obj


def emulate_boundary(props_obj: Any, gem: "GemInfo") -> Any:
    """Emulate the Sandbox serialization round trip of the properties."""
    if not dataclasses.is_dataclass(props_obj):
        return props_obj
    gem_module = type(gem.instance).__module__
    return _recurse_dataclass_boundary(props_obj, gem_module)


# =============================================================================
# Stage "onChange": schema analysis
# =============================================================================

def _literal_of_param(env: jinja2.Environment, params: List["ResolvedParam"],
                      name: str) -> Tuple[Any, bool]:
    """Return (literal_value, present); a non-literal value yields (None, True)."""
    for p in params:
        if p.name == name:
            node = _parse_expr(env, p.value)
            if node is None:
                return None, True
            lit = literal_value(node)
            if lit is _NON_LITERAL:
                return None, True
            return lit, True
    return None, False


def emulate_onchange(gem: "GemInfo", props_obj: Any,
                     resolved_params: List["ResolvedParam"],
                     env: jinja2.Environment, warnings: List[str],
                     info: Optional[Dict[str, Any]] = None) -> Any:
    """Run the gem's onChange against a context synthesized from the call.

    The input schema comes from the call's ``schema_columns`` (or ``schema``)
    and the upstream nodes from ``relation_name``. Returns the new properties.
    """
    import json as _json
    from prophecy.cb.sql.Component import (
        Component, NodePort, NodePorts, SqlNodeMetadata)
    from prophecy.cb.sql.SqlContext import SqlContext, SqlGraph, NodeConnection

    # ---- synthesize the input-port schema columns from the original call ----
    fields: List[Dict[str, Any]] = []
    sc_lit, sc_present = _literal_of_param(env, resolved_params, "schema_columns")
    s_lit, s_present = _literal_of_param(env, resolved_params, "schema")
    if isinstance(s_lit, str):  # gems usually store schema as a JSON string
        try:
            s_lit = _json.loads(s_lit)
        except ValueError:
            pass
    if sc_present:
        if isinstance(sc_lit, (list, tuple)):
            fields = [{"name": str(n), "dataType": {"type": "string"}}
                      for n in sc_lit]
    elif s_present and isinstance(s_lit, (list, tuple)):
        for d in s_lit:
            if isinstance(d, dict):
                fields.append({"name": d.get("name"),
                               "dataType": {"type": d.get("dataType", "string")}})
    else:
        warnings.append(
            "onChange: the call carries no 'schema_columns'/'schema' param; "
            "synthesized an EMPTY input-port schema — a gem that reads the "
            "input schema will see no columns")

    # ---- upstream relations from the relation_name param ----
    rel_lit, rel_present = _literal_of_param(env, resolved_params, "relation_name")
    if not rel_present:
        relations: List[str] = []
    elif isinstance(rel_lit, (list, tuple)):
        relations = [str(r) for r in rel_lit]
    elif isinstance(rel_lit, str):
        relations = [rel_lit]
    else:
        relations = []

    if info is not None:
        info["columns"] = [f.get("name") for f in fields]
        info["relations"] = relations

    schema_json = _json.dumps({"fields": fields})
    inputs = []
    nodes: Dict[str, Any] = {}
    connections = []
    for i, rel in enumerate(relations):
        pid = f"in{i}"
        nid = f"node{i}"
        inputs.append(NodePort(id=pid, slug=pid, schema=schema_json))
        nodes[nid] = SqlNodeMetadata(label=rel)
        connections.append(NodeConnection(
            id=f"conn{i}", source=nid, sourcePort="out0",
            target="c1", targetPort=pid))

    new_state = Component(
        id="c1", component=gem.gem_name,
        metadata=SqlNodeMetadata(label=gem.gem_name),
        ports=NodePorts(inputs=inputs, outputs=[]), properties=props_obj)
    context = SqlContext(
        graph=SqlGraph(connections=connections, nodes=nodes),
        projectName=gem.project_name, projectMacros=[], dependencyProjectMacros={})

    with contextlib.redirect_stdout(io.StringIO()):
        result = gem.instance.onChange(context, new_state, new_state)
    return result.properties


# =============================================================================
# Per-call runner (the full pipeline)
# =============================================================================

@dataclass
class CallResult:
    file: str
    lineno: int
    macro: str  # fq name project.gem
    stage: str  # extract|resolve|validate|loadProperties|rehydrate|onChange|typecheck|apply|compare
    passed: bool
    message: str
    traceback: Optional[str] = None
    params: List[Tuple[str, str]] = field(default_factory=list)  # (name, value)
    orig_sql: Optional[str] = None
    regen_sql: Optional[str] = None


def _sig_default_or_none(sig: Optional[MacroSignature], name: str) -> Optional[str]:
    if sig is None:
        return None
    return sig.defaults.get(name)


def run_call(gc: GemCall, project: Project, file_path: str,
             file_warnings: List[str]) -> CallResult:
    from prophecy.cb.sql.Component import BasicMacroProperties, MacroParameter, MacroProperties

    env = _make_env()
    warnings: List[str] = []

    def result(stage, passed, message, tb=None, params=None, orig_sql=None, regen_sql=None):
        return CallResult(
            file=file_path,
            lineno=gc.lineno,
            macro=gc.fq,
            stage=stage,
            passed=passed,
            message=message,
            traceback=tb,
            params=params or [],
            orig_sql=orig_sql,
            regen_sql=regen_sql,
        )

    gem = project.gems.get((gc.project, gc.gem))
    if gem is None:
        return result("extract", False,
                      f"gem '{gc.gem}' could not be loaded: "
                      f"{project.failed_gems.get(gc.gem, 'unknown load failure')}")
    sig = project.signatures.get(gc.gem)

    # ---- resolve -------------------------------------------------------------
    try:
        params = resolve_params(gc.node, sig, warnings)
    except GenerateCodeError as e:
        return result("resolve", False,
                      f"unrenderable argument node {e.node_type} at line {e.lineno}",
                      tb=None)
    param_pairs = [(p.name, p.value) for p in params]
    file_warnings.extend(warnings)

    # ---- validate ------------------------------------------------------------
    errors = validate_params(gc.node, sig, params, gem, env)
    if errors:
        return result("validate", False,
                      "validation failed:\n  - " + "\n  - ".join(errors),
                      params=param_pairs)

    # ---- loadProperties ------------------------------------------------------
    macro_params = [MacroParameter(p.name, p.value) for p in params if p.name != ""]
    basic = BasicMacroProperties(
        macroName=gem.gem_name, projectName=gem.project_name, parameters=macro_params,
    )
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            props = gem.instance.loadProperties(basic)
    except Exception:
        return result("loadProperties", False,
                      "loadProperties raised an exception",
                      tb=traceback.format_exc(), params=param_pairs)

    # ---- rehydrate -----------------------------------------------------------
    try:
        props = emulate_boundary(props, gem)
    except BoundaryRehydrationError as e:
        return result("rehydrate", False, str(e), params=param_pairs)
    except Exception:
        return result("rehydrate", False,
                      "serialization-boundary rehydration raised an exception",
                      tb=traceback.format_exc(), params=param_pairs)

    # ---- onChange ------------------------------------------------------------
    onchange_warnings: List[str] = []
    onchange_info: Dict[str, Any] = {}
    try:
        props = emulate_onchange(gem, props, params, env,
                                 onchange_warnings, onchange_info)
    except Exception:
        cols = onchange_info.get("columns", [])
        rels = onchange_info.get("relations", [])
        return result("onChange", False,
                      "onChange raised an exception; the context was "
                      "synthesized from the call "
                      f"(schema columns: {cols}; upstream relations: {rels})",
                      tb=traceback.format_exc(), params=param_pairs)
    file_warnings.extend(onchange_warnings)

    # ---- typecheck -----------------------------------------------------------
    tc_violations: List[str] = []
    if not isinstance(props, MacroProperties):
        tc_violations.append(
            f"loadProperties returned {type(props).__name__}, not a MacroProperties"
        )
    if gem.properties_cls is not None and not isinstance(props, gem.properties_cls):
        tc_violations.append(
            f"loadProperties returned {type(props).__name__}, expected "
            f"{gem.properties_cls.__name__}"
        )
    if dataclasses.is_dataclass(props):
        _typecheck_dataclass(props, "", tc_violations)

    # ---- apply (runs even on typecheck errors so both surface) ---------------
    apply_tb = None
    apply_out = None
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            apply_out = gem.instance.apply(props)
        if not isinstance(apply_out, str):
            raise TypeError(f"apply() returned {type(apply_out).__name__}, expected str")
    except Exception:
        apply_tb = traceback.format_exc()

    if apply_tb is not None:
        msg = "apply() raised an exception"
        if tc_violations:
            msg += "\ntypecheck violations (dataclass fields vs annotations):\n  - " \
                   + "\n  - ".join(tc_violations)
        return result("apply", False, msg, tb=apply_tb, params=param_pairs)

    if tc_violations:
        return result("typecheck", False,
                      "typecheck violations (dataclass fields vs annotations):\n  - "
                      + "\n  - ".join(tc_violations),
                      params=param_pairs)

    # ---- compare -------------------------------------------------------------
    try:
        regen_ast = env.parse(apply_out)
    except jinja2.exceptions.TemplateSyntaxError as e:
        return result("compare", False,
                      f"regenerated macro call does not parse: {e.message}\n"
                      f"  output: {apply_out}", params=param_pairs)

    regen_calls = [c for c in regen_ast.find_all(nodes.Call)
                   if _callee_names(c, project.project_name) == (gc.project, gc.gem)]
    if len(regen_calls) != 1:
        return result("compare", False,
                      f"regenerated output must contain exactly one call to "
                      f"{gc.fq}, found {len(regen_calls)}\n  output: {apply_out}",
                      params=param_pairs)

    try:
        regen_params = resolve_params(regen_calls[0], sig, [])
    except GenerateCodeError as e:
        return result("compare", False,
                      f"regenerated call contains an unrenderable argument node "
                      f"{e.node_type} at line {e.lineno}\n  output: {apply_out}",
                      params=param_pairs)
    orig_map = {p.name: p.value for p in params if p.name != ""}
    regen_map = {p.name: p.value for p in regen_params if p.name != ""}

    def param_matches(name: str) -> bool:
        o = orig_map.get(name)
        r = regen_map.get(name)
        if o is not None and r is not None:
            return values_equal(env, o, r)
        # A param on only one side matches only the signature default.
        present = o if o is not None else r
        default = _sig_default_or_none(sig, name)
        return default is not None and values_equal(env, present, default)

    if not all(param_matches(n) for n in set(orig_map) | set(regen_map)):
        return result(
            "compare",
            False,
            "generated SQL does not match the original call (run with -v to see the SQL)",
            params=param_pairs,
            orig_sql="{{ " + generate_code(gc.node) + " }}",
            regen_sql="{{ " + generate_code(regen_calls[0]) + " }}")

    return result("compare", True, "round-trip verified", params=param_pairs)


# =============================================================================
# File-level driver
# =============================================================================

@dataclass
class FileReport:
    path: str
    passed: bool
    results: List[CallResult]
    warnings: List[str]
    no_calls: bool = False
    fatal: Optional[str] = None
    project_errors: List[str] = field(default_factory=list)


def test_file(path: str, project_dir: Optional[str] = None) -> FileReport:
    path = os.path.abspath(path)
    warnings: List[str] = []
    try:
        if project_dir is None:
            project_dir = find_project_dir(path)
        project = load_project(project_dir)
    except Exception as e:
        return FileReport(path=path, passed=False, results=[], warnings=[],
                          fatal=f"{type(e).__name__}: {e}")

    warnings.extend(f"[project] {w}" for w in project.warnings)

    try:
        gem_calls = extract_calls(path, project, warnings)
    except Exception as e:
        return FileReport(path=path, passed=False, results=[], warnings=warnings,
                          fatal=f"extraction failed: {type(e).__name__}: {e}")

    if not gem_calls:
        return FileReport(path=path, passed=not project.errors, results=[],
                          warnings=warnings, no_calls=True,
                          project_errors=list(project.errors))

    results: List[CallResult] = []
    for gc in gem_calls:
        try:
            results.append(run_call(gc, project, path, warnings))
        except Exception:
            # A harness bug must fail the call visibly, never abort the batch.
            results.append(CallResult(
                file=path, lineno=gc.lineno, macro=gc.fq, stage="harness",
                passed=False,
                message="internal error while testing this call "
                        "(bug in code_to_visual_runner.py, not the gem)",
                traceback=traceback.format_exc(),
            ))
    passed = all(r.passed for r in results) and not project.errors
    return FileReport(path=path, passed=passed, results=results, warnings=warnings,
                      project_errors=list(project.errors))


# =============================================================================
# CLI & Reporting
# =============================================================================

def print_report(report: FileReport, verbose: bool, out=sys.stdout) -> None:
    rel = report.path
    print(f"\n=== {rel} ===", file=out)

    if report.fatal:
        print(f"[FATAL] {report.fatal}", file=out)

    for e in report.project_errors:
        print(f"[error] [project] {e}", file=out)
    for w in report.warnings:
        print(f"[warn] {w}", file=out)

    if report.no_calls:
        print("no gem macro calls found in this file", file=out)
        return

    for r in report.results:
        tag = "PASS" if r.passed else "FAIL"
        print(f"[{tag}] {os.path.basename(r.file)}:{r.lineno} {r.macro}", file=out)
        if verbose:
            print("  resolved parameters:", file=out)
            for name, value in r.params:
                print(f"    {name or '<positional>'} = {value}", file=out)
        if not r.passed:
            print(f"  stage: {r.stage}", file=out)
            for line in r.message.splitlines():
                print(f"  {line}", file=out)
            if verbose and r.orig_sql and r.regen_sql:
                print(f"  - {r.orig_sql}", file=out)
                print(f"  + {r.regen_sql}", file=out)
            if r.traceback:
                print("  traceback:", file=out)
                for line in r.traceback.rstrip().splitlines():
                    print(f"    {line}", file=out)


# Directories skipped when expanding a directory argument.
SKIP_DIRS = {"target", "dbt_packages", ".git", "__pycache__",
             "node_modules", ".venv", "venv", "env", "logs"}


def expand_paths(paths: List[str]) -> Tuple[List[str], List[str]]:
    """Expand CLI path args, a directory becomes all *.sql files under it
    (recursive, sorted, skipping ``SKIP_DIRS``).

    Returns (sql_files, errors): errors are messages for paths that do not
    exist or directories that contain no .sql files (loud, never silent).
    """
    files: List[str] = []
    errors: List[str] = []
    for path in paths:
        if os.path.isdir(path):
            found = []
            for dp, dn, fns in os.walk(path):
                dn[:] = [d for d in dn if d not in SKIP_DIRS]
                found.extend(os.path.join(dp, f) for f in fns if f.endswith(".sql"))
            if not found:
                errors.append(f"no .sql files under directory: {path}")
            files.extend(sorted(found))
        elif os.path.isfile(path):
            files.append(path)
        else:
            errors.append(f"path not found: {path}")
    return files, errors


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Code-to-Visual tester for Prophecy SQL macro gems.")
    parser.add_argument(
        "files",
        nargs="+",
        help="one or more sql files to test, or a directory that is searched recursively for *.sql files.")
    parser.add_argument(
        "--project-dir",
        default=None,
        help="project root containing dbt_project.yml")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="also print the resolved MacroParameters per call")
    args = parser.parse_args(argv)

    total_calls = 0
    total_passed = 0
    total_failed = 0
    any_fatal = False
    all_project_warnings: List[str] = []
    all_project_errors: List[str] = []

    sql_files, expand_errors = expand_paths(args.files)
    for e in expand_errors:
        print(f"\n[FATAL] {e}", file=sys.stdout)
        any_fatal = True

    for f in sql_files:
        report = test_file(f, args.project_dir)
        print_report(report, args.verbose)
        if report.fatal:
            any_fatal = True
        passed = len(list(filter(lambda r: r.passed, report.results)))
        total_calls += len(report.results)
        total_passed += passed
        total_failed += len(report.results) - passed
        for w in report.warnings:
            if w.startswith("[project]") and w not in all_project_warnings:
                all_project_warnings.append(w)
        for e in report.project_errors:
            if e not in all_project_errors:
                all_project_errors.append(e)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"  files tested : {len(sql_files)}")
    print(f"  gem calls    : {total_calls}")
    print(f"  passed       : {total_passed}")
    print(f"  failed       : {total_failed}")
    if all_project_errors:
        print("  project ERRORS (gems that could not be loaded):")
        for e in all_project_errors:
            print(f"    - {e}")
    if all_project_warnings:
        print("  project warnings:")
        for w in all_project_warnings:
            print(f"    - {w[len('[project] '):]}")
    print("=" * 60)

    if any_fatal or total_failed > 0 or all_project_errors:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
