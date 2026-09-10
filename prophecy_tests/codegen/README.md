# Gem SQL serialization tests

Run from the package root with the Python sandbox environment (Prophecy
component-builder SDK, Jinja2, and pytest installed):

```sh
python -m pytest prophecy_tests/codegen -q
```

These tests import the actual gem class and use Jinja's parser and renderer.
They do not start Spark or connect to a warehouse. They verify the exact SQL
expression passed into MultiColumnRename, source-token loading, legacy raw
`_oldMacroProperties`, and repeated save/load cycles. Nonconstant native Jinja
arguments are rejected rather than silently converted into literal SQL.

Rename choices are registered in `MultiColumnRename.RENAME_METHODS`. Both the
UI dropdown and loader derive their supported choices from this mapping. The
tests cover both current modes, the unselected default, and a newly registered
choice in both generated-code and saved-property formats. A new method still
needs its actual rename behavior implemented; it needs no separate loader list.
