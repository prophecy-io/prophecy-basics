"""Helpers for gem-level tests that check loadProperties/unloadProperties directly.

cases.yml runs onChange after reopening the code, which rebuilds anything taken
from the input port; these tests look at what loadProperties alone returns.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import code_to_visual_runner as c2v  # noqa: E402


@pytest.fixture(scope="session")
def project():
    pytest.importorskip("prophecy")
    return c2v.load_project(c2v.find_project_dir(__file__))


@pytest.fixture
def reopen(project):
    """(gem, properties) for ``code`` as the SQL Editor loads it, before onChange.

    Arguments are named from the macro signature; string literals keep their quotes.
    """
    from prophecy.cb.sql.Component import BasicMacroProperties, MacroParameter

    def _reopen(code):
        calls = c2v.extract_calls_from_text(code, project, [])
        assert len(calls) == 1, f"expected one gem call in {code!r}"
        gc = calls[0]
        gem = project.gems[(gc.project, gc.gem)]
        params = c2v.resolve_params(gc.node, project.signatures.get(gc.gem), [])
        props = gem.instance.loadProperties(BasicMacroProperties(
            macroName=gem.gem_name, projectName=gem.project_name,
            parameters=[MacroParameter(p.name, p.value) for p in params if p.name]))
        return gem.instance, props

    return _reopen


@pytest.fixture
def saved_state():
    """unloadProperties output as a {name: value} dict, and a way to load it back."""
    from prophecy.cb.sql.Component import BasicMacroProperties, MacroParameter

    class SavedState:
        @staticmethod
        def unload(gem, props):
            return {p.name: p.value for p in gem.unloadProperties(props).parameters}

        @staticmethod
        def load(gem, values):
            return gem.loadProperties(BasicMacroProperties(
                macroName=gem.name, projectName=gem.projectName,
                parameters=[MacroParameter(k, v) for k, v in values.items()]))

    return SavedState
