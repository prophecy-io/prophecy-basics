"""
Shared pytest fixtures and utilities for Spark tests.
This file is automatically discovered by pytest and provides fixtures to all test files.
"""

import pytest
import sys
import os
import dataclasses
from typing import List
from unittest.mock import Mock, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def setup_prophecy_mocks():
    """
    Setup mock objects for Prophecy framework dependencies.
    This allows importing gem classes without the actual Prophecy framework.
    This function is called automatically before tests run via the autouse fixture.
    """
    # Create mock classes for Prophecy framework
    class MockMacroProperties:
        """Mock MacroProperties class"""
        pass

    class MockMacroSpec:
        """Mock MacroSpec base class"""
        pass

    class MockProviderTypeEnum:
        Databricks = "Databricks"
        Snowflake = "Snowflake"
        BigQuery = "BigQuery"
        ProphecyManaged = "ProphecyManaged"

    class MockMacroBuilderBase:
        MacroSpec = MockMacroSpec
        MacroProperties = MockMacroProperties
        SqlContext = MagicMock
        Component = MagicMock
        Diagnostic = MagicMock
        SeverityLevelEnum = MagicMock
        PropertiesType = MagicMock

    class MockUISpec:
        ProviderTypeEnum = MockProviderTypeEnum
        Dialog = MagicMock
        RadioGroup = MagicMock
        SchemaColumnsDropdown = MagicMock
        AlertBox = MagicMock
        TitleElement = MagicMock
        Markdown = MagicMock
        TextBox = MagicMock
        SelectBox = MagicMock
        ExpressionBox = MagicMock
        StackLayout = MagicMock
        ColumnsLayout = MagicMock
        SqlContext = MagicMock
        Component = MagicMock
        Diagnostic = MagicMock
        SeverityLevelEnum = MagicMock

    # Register mocks in sys.modules so "from X import *" works
    mock_macro_base_mod = type(sys)('prophecy.cb.sql.MacroBuilderBase')
    mock_macro_base_mod.__dict__.update(MockMacroBuilderBase.__dict__)
    
    # Mock ComponentBuilderBase (used by UnionByName)
    mock_component_builder_base = MagicMock()
    
    sys.modules['prophecy'] = type(sys)('prophecy')
    sys.modules['prophecy.cb'] = type(sys)('prophecy.cb')
    sys.modules['prophecy.cb.sql'] = type(sys)('prophecy.cb.sql')
    sys.modules['prophecy.cb.sql.MacroBuilderBase'] = mock_macro_base_mod
    sys.modules['prophecy.cb.server'] = type(sys)('prophecy.cb.server')
    sys.modules['prophecy.cb.server.base'] = type(sys)('prophecy.cb.server.base')
    mock_component_builder_mod = type(sys)('prophecy.cb.server.base.ComponentBuilderBase')
    mock_component_builder_mod.__dict__.update(mock_component_builder_base.__dict__)
    sys.modules['prophecy.cb.server.base.ComponentBuilderBase'] = mock_component_builder_mod
    sys.modules['prophecy.cb.ui'] = type(sys)('prophecy.cb.ui')
    mock_uispec_mod = type(sys)('prophecy.cb.ui.uispec')
    mock_uispec_mod.__dict__.update(MockUISpec.__dict__)
    sys.modules['prophecy.cb.ui.uispec'] = mock_uispec_mod

    # Make dataclass and field available in builtins (CountRecords uses @dataclass)
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
    This fixture is session-scoped, meaning it's created once per test session
    and reused across all tests for efficiency.
    """
    spark = SparkSession.builder \
        .appName("ProphecyBasicsTests") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .getOrCreate()
    
    # Set log level to WARN to reduce output noise during tests
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    spark.stop()


def import_gem_class(gem_name: str):
    """
    Helper function to import a gem class by name.
    
    Args:
        gem_name: Name of the gem class (e.g., "CountRecords")
        
    Returns:
        The gem class
        
    Example:
        CountRecords = import_gem_class("CountRecords")
        instance = CountRecords()
        instance.props = Mock()
        instance.props.column_names = []
    """
    # Ensure mocks are set up
    if 'prophecy.cb.sql.MacroBuilderBase' not in sys.modules:
        setup_prophecy_mocks()
    
    # Import the class
    module = __import__(gem_name, fromlist=[gem_name])
    return getattr(module, gem_name)

