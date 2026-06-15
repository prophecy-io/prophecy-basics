"""
Shared pytest fixtures for testing gems.
Sets up Prophecy framework mocks and provides SparkSession fixture.
This conftest is specific to ui_spec.py file
"""

import os
import sys
import dataclasses
from typing import List
from unittest.mock import MagicMock
import pytest
from pyspark.sql import SparkSession


def setup_prophecy_mocks():
    """Mock Prophecy framework so gems can be imported."""
    # Create minimal mock classes
    class MockMacroSpec:
        pass
    
    class MockMacroProperties:
        pass
    
    class MockProviderTypeEnum:
        Databricks = "Databricks"
        Snowflake = "Snowflake"
        BigQuery = "BigQuery"
        ProphecyManaged = "ProphecyManaged"
    
    from pyspark.sql import DataFrame

    # Create mock modules
    mock_macro_base = type(sys)('prophecy.cb.sql.MacroBuilderBase')
    mock_macro_base.DataFrame = DataFrame
    mock_macro_base.MacroSpec = MockMacroSpec
    mock_macro_base.MacroProperties = MockMacroProperties
    mock_macro_base.Component = MagicMock
    mock_macro_base.SqlContext = MagicMock
    mock_macro_base.Diagnostic = MagicMock
    mock_macro_base.SeverityLevelEnum = MagicMock
    mock_macro_base.PropertiesType = MagicMock
    
    mock_uispec = type(sys)('prophecy.cb.ui.uispec')
    mock_uispec.ProviderTypeEnum = MockProviderTypeEnum
    mock_uispec.Dialog = MagicMock
    mock_uispec.RadioGroup = MagicMock
    mock_uispec.SchemaColumnsDropdown = MagicMock
    mock_uispec.Checkbox = MagicMock
    mock_uispec.ColumnsLayout = MagicMock
    mock_uispec.StackLayout = MagicMock
    mock_uispec.TextBox = MagicMock
    mock_uispec.SelectBox = MagicMock
    mock_uispec.Condition = MagicMock
    mock_uispec.PropExpr = MagicMock
    mock_uispec.StringExpr = MagicMock
    
    # Register all Prophecy modules as mocks
    sys.modules['prophecy'] = type(sys)('prophecy')
    sys.modules['prophecy.cb'] = type(sys)('prophecy.cb')
    sys.modules['prophecy.cb.sql'] = type(sys)('prophecy.cb.sql')
    sys.modules['prophecy.cb.sql.MacroBuilderBase'] = mock_macro_base
    sys.modules['prophecy.cb.ui'] = type(sys)('prophecy.cb.ui')
    sys.modules['prophecy.cb.ui.uispec'] = mock_uispec
    
    # Make dataclass available in builtins
    import builtins
    if not hasattr(builtins, 'dataclass'):
        builtins.dataclass = dataclasses.dataclass
    if not hasattr(builtins, 'field'):
        builtins.field = dataclasses.field
    if 'List' not in dir(builtins):
        builtins.List = List
    
    # Add gems directory to Python path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    gems_dir = os.path.join(current_dir, '../../gems')
    if gems_dir not in sys.path:
        sys.path.insert(0, gems_dir)


# Setup mocks before any imports
setup_prophecy_mocks()


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    Session-scoped: created once and reused across all tests.
    """
    spark_session = (SparkSession.builder
        .appName("PySparkGemTests")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate())
    
    spark_session.sparkContext.setLogLevel("WARN")
    
    yield spark_session
    
    spark_session.stop()
