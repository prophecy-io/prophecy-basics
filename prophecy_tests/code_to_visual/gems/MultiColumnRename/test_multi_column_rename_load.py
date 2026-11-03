"""MultiColumnRename: the column list must survive loadProperties on its own.

The macro's schema argument holds only the column names, e.g. ['a', 'b'];
saved state holds schema as JSON with data types. Both arrive as "schema".
"""
import json
from dataclasses import replace

CODE = ("{{ prophecy_basics.MultiColumnRename(['source'],['age'],'editPrefixSuffix',"
        "['first_name', 'age'],'Suffix','_years',\"\") }}")


def test_reopened_code_keeps_the_column_list(reopen):
    gem, props = reopen(CODE)
    assert json.loads(props.schema) == [{"name": "first_name"}, {"name": "age"}]
    assert gem.apply(props) == CODE


def test_saved_state_keeps_the_typed_schema(reopen, saved_state):
    gem, props = reopen(CODE)
    typed = replace(props, schema=json.dumps([{"name": "age", "dataType": "Integer"}]))
    assert saved_state.load(gem, saved_state.unload(gem, typed)) == typed


def test_missing_schema_is_saved_as_empty(reopen, saved_state):
    gem, props = reopen(CODE)
    assert saved_state.unload(gem, replace(props, schema=None))["schema"] == ""


def test_schema_saved_as_none_by_older_versions_loads_as_empty(reopen, saved_state):
    gem, props = reopen(CODE)
    values = saved_state.unload(gem, props)
    values["schema"] = "None"
    assert saved_state.load(gem, values).schema == ""
