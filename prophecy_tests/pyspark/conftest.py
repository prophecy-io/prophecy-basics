"""
Shared pytest fixtures and configuration for Spark tests.
This file sets up Prophecy framework mocks and provides a SparkSession fixture.
"""

import os
import sys
import dataclasses
from typing import List
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession


def setup_prophecy_mocks():
    """
    Setup mocks for Prophecy framework dependencies.
    This allows testing gem applyPython methods without the full Prophecy environment.
    """
    # Create a mock MacroProperties class
    class MockMacroProperties:
        """Mock MacroProperties class"""
        pass

    # Create a mock MacroSpec class
    class MockMacroSpec:
        """Mock MacroSpec base class"""
        pass

    # Create mock ProviderTypeEnum
    class MockProviderTypeEnum:
        Databricks = "Databricks"
        Snowflake = "Snowflake"
        BigQuery = "BigQuery"
        ProphecyManaged = "ProphecyManaged"

    # Create mock module objects that support wildcard imports
    class MockMacroBuilderBase:
        MacroSpec = MockMacroSpec
        MacroProperties = MockMacroProperties
        # Add common types used in MacroSpec classes
        SqlContext = MagicMock
        Component = MagicMock
        Diagnostic = MagicMock
        SeverityLevelEnum = MagicMock
        PropertiesType = MagicMock  # Used in type hints

    class MockUISpec:
        ProviderTypeEnum = MockProviderTypeEnum
        Dialog = MagicMock
        RadioGroup = MagicMock
        SchemaColumnsDropdown = MagicMock
        # Add other UI components that might be imported
        AlertBox = MagicMock
        TitleElement = MagicMock
        Markdown = MagicMock
        TextBox = MagicMock
        SelectBox = MagicMock
        ExpressionBox = MagicMock
        StackLayout = MagicMock
        ColumnsLayout = MagicMock
        # Additional types that might be needed
        SqlContext = MagicMock
        Component = MagicMock
        Diagnostic = MagicMock
        SeverityLevelEnum = MagicMock

    # Register mocks in sys.modules so "from X import *" works
    mock_macro_base_mod = type(sys)('prophecy.cb.sql.MacroBuilderBase')
    mock_macro_base_mod.__dict__.update(MockMacroBuilderBase.__dict__)

    # Mock ComponentBuilderBase (used by UnionByName)
    mock_component_builder_base = MagicMock()

    # Mock Component module (used by TextToColumns)
    mock_component_mod = type(sys)('prophecy.cb.sql.Component')
    mock_component_mod.__dict__.update({})  # Empty dict for now, wildcard imports will work
    
    sys.modules['prophecy'] = type(sys)('prophecy')
    sys.modules['prophecy.cb'] = type(sys)('prophecy.cb')
    sys.modules['prophecy.cb.sql'] = type(sys)('prophecy.cb.sql')
    sys.modules['prophecy.cb.sql.MacroBuilderBase'] = mock_macro_base_mod
    sys.modules['prophecy.cb.sql.Component'] = mock_component_mod
    sys.modules['prophecy.cb.server'] = type(sys)('prophecy.cb.server')
    sys.modules['prophecy.cb.server.base'] = type(sys)('prophecy.cb.server.base')
    mock_component_builder_mod = type(sys)('prophecy.cb.server.base.ComponentBuilderBase')
    mock_component_builder_mod.__dict__.update(mock_component_builder_base.__dict__)
    sys.modules['prophecy.cb.server.base.ComponentBuilderBase'] = mock_component_builder_mod
    sys.modules['prophecy.cb.ui'] = type(sys)('prophecy.cb.ui')
    mock_uispec_mod = type(sys)('prophecy.cb.ui.uispec')
    mock_uispec_mod.__dict__.update(MockUISpec.__dict__)
    sys.modules['prophecy.cb.ui.uispec'] = mock_uispec_mod

    # Make dataclass and field available in builtins (gems use @dataclass)
    import builtins
    if not hasattr(builtins, 'dataclass'):
        builtins.dataclass = dataclasses.dataclass
    if not hasattr(builtins, 'field'):
        builtins.field = dataclasses.field
    if 'List' not in dir(builtins):
        builtins.List = List

    # Add gems directory to Python path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(current_dir, '../..')
    gems_dir = os.path.join(project_root, 'gems')
    if gems_dir not in sys.path:
        sys.path.insert(0, gems_dir)


# Setup mocks at module import time (before any test imports happen)
# This ensures mocks are ready when test files import gem classes
setup_prophecy_mocks()


@pytest.fixture(scope="session", autouse=True)
def setup_prophecy_mocks_fixture():
    """
    Automatically setup Prophecy mocks before any test runs.
    This fixture runs once per test session (autouse=True means it runs automatically).
    Note: setup_prophecy_mocks() is also called at module level to ensure mocks
    are ready when test files import gem classes during collection.
    """
    # Mocks are already set up at module level, but ensure they're set up here too
    # (useful if this fixture runs before module-level execution)
    if 'prophecy.cb.sql.MacroBuilderBase' not in sys.modules:
        setup_prophecy_mocks()
    yield
    # Cleanup (if needed) happens here


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    This fixture is session-scoped, so it's created once and reused across all tests.
    """
    spark_session = SparkSession.builder \
        .appName("PySparkGemTests") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()
    
    spark_session.sparkContext.setLogLevel("WARN")
    
    yield spark_session
    
    spark_session.stop()


def import_gem_class(gem_name: str):
    """
    Helper function to import a gem class.
    
    Args:
        gem_name: Name of the gem file without .py extension (e.g., 'CountRecords')
    
    Returns:
        The imported class
    """
    module = __import__(gem_name, fromlist=[gem_name])
    return getattr(module, gem_name)

