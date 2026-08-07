import pytest
from unittest.mock import Mock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from DataMasking import DataMasking


@pytest.fixture
def create_gem():
    """Factory fixture to create a DataMasking gem with mocked props."""
    def _create(**kwargs):
        gem = DataMasking()
        gem.props = Mock()

        gem.props.column_names = kwargs.get("column_names", [])
        gem.props.masking_method = kwargs.get("masking_method", "")
        gem.props.masked_column_add_method = kwargs.get("masked_column_add_method", "inplace_substitute")
        gem.props.upper_char_substitute = kwargs.get("upper_char_substitute", "")
        gem.props.lower_char_substitute = kwargs.get("lower_char_substitute", "")
        gem.props.digit_char_substitute = kwargs.get("digit_char_substitute", "")
        gem.props.other_char_substitute = kwargs.get("other_char_substitute", "")
        gem.props.sha2_bit_length = kwargs.get("sha2_bit_length", "256")
        gem.props.prefix_suffix_option = kwargs.get("prefix_suffix_option", "Prefix")
        gem.props.prefix_suffix_added = kwargs.get("prefix_suffix_added", "")
        gem.props.combined_hash_column_name = kwargs.get("combined_hash_column_name", "")

        return gem
    return _create


@pytest.fixture
def sample_df(spark):
    """Sample DataFrame with various data types."""
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("password", StringType(), True),
        StructField("id", IntegerType(), True)
    ])
    data = [("John123", "SecretPass#456", 1)]
    return spark.createDataFrame(data, schema)
