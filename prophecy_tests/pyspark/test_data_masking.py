import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from unittest.mock import Mock
import sys
import os

# Import the helper to get the gem class
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(current_dir, 'conftest.py'))

from conftest import import_gem_class

DataMasking = import_gem_class('DataMasking')


def create_data_masking_instance(
    column_names,
    masking_method,
    masked_column_add_method="inplace_substitute",
    upper_char_substitute="",
    lower_char_substitute="",
    digit_char_substitute="",
    other_char_substitute="",
    sha2_bit_length="256",
    prefix_suffix_option="Prefix",
    prefix_suffix_added="",
    combined_hash_column_name=""
):
    """Helper function to create a DataMasking instance with properties."""
    instance = DataMasking()
    instance.props = Mock()
    instance.props.column_names = column_names
    instance.props.masking_method = masking_method
    instance.props.masked_column_add_method = masked_column_add_method
    instance.props.upper_char_substitute = upper_char_substitute
    instance.props.lower_char_substitute = lower_char_substitute
    instance.props.digit_char_substitute = digit_char_substitute
    instance.props.other_char_substitute = other_char_substitute
    instance.props.sha2_bit_length = sha2_bit_length
    instance.props.prefix_suffix_option = prefix_suffix_option
    instance.props.prefix_suffix_added = prefix_suffix_added
    instance.props.combined_hash_column_name = combined_hash_column_name
    return instance


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("test_data_masking") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


class TestDataMaskingMaskMethod:
    """Test cases for mask masking method with character replacements."""
    
    def test_mask_inplace_default_characters(self, spark):
        """Test mask method with default character substitutions (inplace)."""
        data = [("John123", "SecretPass#456", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("password", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name", "password"],
            masking_method="mask",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 1
        # Default: uppercase->X, lowercase->x, digits->n, special->unchanged
        assert rows[0].name == "Xxxxnnn"
        assert rows[0].password == "XxxxxxXxxx#nnn"  # SecretPass#456 -> S=X, ecret=xxxxx, P=X, ass=xxx, #=#, 456=nnn
        assert rows[0].id == 1
    
    def test_mask_custom_characters(self, spark):
        """Test mask method with custom character substitutions."""
        data = [("John123",)]
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="mask",
            masked_column_add_method="inplace_substitute",
            upper_char_substitute="A",
            lower_char_substitute="a",
            digit_char_substitute="0",
            other_char_substitute="*"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        assert rows[0].name == "Aaaa000"
    
    def test_mask_null_preserves_characters(self, spark):
        """Test mask method with NULL preserves original characters."""
        data = [("John123!",)]
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="mask",
            masked_column_add_method="inplace_substitute",
            upper_char_substitute="NULL",  # Preserve uppercase
            lower_char_substitute="NULL",  # Preserve lowercase
            digit_char_substitute="0",     # Mask digits
            other_char_substitute="NULL"   # Preserve special chars
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        assert rows[0].name == "John000!"
    
    def test_mask_with_prefix(self, spark):
        """Test mask method with prefix substitution."""
        data = [("John123", "Alice456", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("username", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name", "username"],
            masking_method="mask",
            masked_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Prefix",
            prefix_suffix_added="masked_"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 1
        # Original columns should be preserved
        assert rows[0].name == "John123"
        assert rows[0].username == "Alice456"
        # New masked columns should be added
        assert rows[0].masked_name == "Xxxxnnn"
        assert rows[0].masked_username == "Xxxxxnnn"
        assert rows[0].id == 1
    
    def test_mask_with_suffix(self, spark):
        """Test mask method with suffix substitution."""
        data = [("John123",)]
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="mask",
            masked_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Suffix",
            prefix_suffix_added="_masked"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # Original column should be preserved
        assert rows[0].name == "John123"
        # New masked column should be added
        assert rows[0].name_masked == "Xxxxnnn"


class TestDataMaskingHashMethod:
    """Test cases for hash masking method."""
    
    def test_hash_inplace_substitute(self, spark):
        """Test hash method with inplace substitution."""
        data = [("John", "john@email.com", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name", "email"],
            masking_method="hash",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 1
        # Check that hash values are integers and not the original strings
        assert isinstance(rows[0].name, int)
        assert isinstance(rows[0].email, int)
        assert rows[0].name != "John"
        assert rows[0].email != "john@email.com"
        assert rows[0].id == 1
    
    def test_hash_with_prefix(self, spark):
        """Test hash method with prefix substitution."""
        data = [("John", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="hash",
            masked_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Prefix",
            prefix_suffix_added="hashed_"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # Original column should be preserved
        assert rows[0].name == "John"
        # New hashed column should be added
        assert isinstance(rows[0].hashed_name, int)
        assert rows[0].id == 1
    
    def test_hash_combined_hash(self, spark):
        """Test hash method with combined hash for multiple columns."""
        data = [("John", "Doe", "john.doe@email.com", 1)]
        schema = StructType([
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["first_name", "last_name", "email"],
            masking_method="hash",
            masked_column_add_method="combinedHash_substitute",
            combined_hash_column_name="combined_hash"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # Original columns should be preserved
        assert rows[0].first_name == "John"
        assert rows[0].last_name == "Doe"
        assert rows[0].email == "john.doe@email.com"
        # Combined hash column should be added
        assert isinstance(rows[0].combined_hash, int)
        assert rows[0].id == 1


class TestDataMaskingSha2Method:
    """Test cases for SHA-2 masking method."""
    
    def test_sha2_inplace_256_bit(self, spark):
        """Test SHA-2 with 256-bit length (inplace)."""
        data = [("John", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="sha2",
            masked_column_add_method="inplace_substitute",
            sha2_bit_length="256"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # SHA2-256 produces a 64-character hex string
        assert len(rows[0].name) == 64
        assert rows[0].name != "John"
        assert rows[0].id == 1
    
    def test_sha2_with_different_bit_lengths(self, spark):
        """Test SHA-2 with different bit lengths."""
        data = [("test",)]
        schema = StructType([StructField("col", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        for bit_length, expected_len in [("224", 56), ("256", 64), ("384", 96), ("512", 128)]:
            masking = create_data_masking_instance(
                column_names=["col"],
                masking_method="sha2",
                masked_column_add_method="inplace_substitute",
                sha2_bit_length=bit_length
            )
            
            result = masking.applyPython(spark, df)
            rows = result.collect()
            
            assert len(rows[0].col) == expected_len, f"SHA2-{bit_length} should produce {expected_len} chars"
    
    def test_sha2_with_prefix(self, spark):
        """Test SHA-2 with prefix substitution."""
        data = [("John",)]
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="sha2",
            masked_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Prefix",
            prefix_suffix_added="sha2_",
            sha2_bit_length="256"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # Original column should be preserved
        assert rows[0].name == "John"
        # New SHA2 column should be added
        assert len(rows[0].sha2_name) == 64


class TestDataMaskingShaMethod:
    """Test cases for SHA (SHA-1) masking method."""
    
    def test_sha_inplace_substitute(self, spark):
        """Test SHA-1 with inplace substitution."""
        data = [("John", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="sha",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # SHA-1 produces a 40-character hex string
        assert len(rows[0].name) == 40
        assert rows[0].name != "John"
        assert rows[0].id == 1


class TestDataMaskingMd5Method:
    """Test cases for MD5 masking method."""
    
    def test_md5_inplace_substitute(self, spark):
        """Test MD5 with inplace substitution."""
        data = [("John", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="md5",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # MD5 produces a 32-character hex string
        assert len(rows[0].name) == 32
        assert rows[0].name != "John"
        assert rows[0].id == 1
    
    def test_md5_with_suffix(self, spark):
        """Test MD5 with suffix substitution."""
        data = [("John",)]
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="md5",
            masked_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Suffix",
            prefix_suffix_added="_md5"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # Original column should be preserved
        assert rows[0].name == "John"
        # New MD5 column should be added
        assert len(rows[0].name_md5) == 32


class TestDataMaskingCrc32Method:
    """Test cases for CRC32 masking method."""
    
    def test_crc32_inplace_substitute(self, spark):
        """Test CRC32 with inplace substitution."""
        data = [("John", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="crc32",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # CRC32 produces an integer
        assert isinstance(rows[0].name, int)
        assert rows[0].name > 0
        assert rows[0].id == 1


class TestDataMaskingEdgeCases:
    """Test edge cases and special scenarios."""
    
    def test_masking_with_null_values(self, spark):
        """Test masking handles NULL values correctly."""
        data = [(None, "John", 1), ("Alice", None, 2)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name", "email"],
            masking_method="mask",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 2
        # NULL values should remain NULL
        assert rows[0].name is None
        assert rows[1].email is None
        # Non-NULL values should be masked
        assert rows[0].email == "Xxxx"
        assert rows[1].name == "Xxxxx"
    
    def test_masking_multiple_columns(self, spark):
        """Test masking multiple columns simultaneously."""
        data = [("John", "john@email.com", "555-1234", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name", "email", "phone"],
            masking_method="md5",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # All selected columns should be hashed
        assert len(rows[0].name) == 32
        assert len(rows[0].email) == 32
        assert len(rows[0].phone) == 32
        # Unselected column should remain unchanged
        assert rows[0].id == 1
    
    def test_schema_preservation(self, spark):
        """Test that the DataFrame schema is preserved correctly."""
        data = [("John", "john@email.com", 1)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="mask",
            masked_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Prefix",
            prefix_suffix_added="masked_"
        )
        
        result = masking.applyPython(spark, df)
        
        # Check that all original columns are present
        assert "name" in result.columns
        assert "email" in result.columns
        assert "id" in result.columns
        # Check that new column is added
        assert "masked_name" in result.columns
    
    def test_empty_dataframe(self, spark):
        """Test masking on an empty DataFrame."""
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame([], schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="hash",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 0
        assert "name" in result.columns
        assert "id" in result.columns
    
    def test_consistency_same_input_same_output(self, spark):
        """Test that masking the same value produces consistent results."""
        data = [("John",), ("Alice",), ("John",)]
        schema = StructType([StructField("name", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["name"],
            masking_method="md5",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # Same input should produce same output
        assert rows[0].name == rows[2].name
        # Different input should produce different output
        assert rows[0].name != rows[1].name
    
    def test_mask_special_characters_only(self, spark):
        """Test masking special characters only."""
        data = [("John@123#",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        masking = create_data_masking_instance(
            column_names=["text"],
            masking_method="mask",
            masked_column_add_method="inplace_substitute",
            upper_char_substitute="NULL",
            lower_char_substitute="NULL",
            digit_char_substitute="NULL",
            other_char_substitute="*"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # Only special characters should be masked
        assert rows[0].text == "John*123*"
    
    def test_mixed_numeric_string_columns(self, spark):
        """Test masking on mixed data types (converted to string for hashing)."""
        data = [(1, "John", "100"), (2, "Alice", "200")]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Hash both integer and string columns
        masking = create_data_masking_instance(
            column_names=["id", "name", "value"],
            masking_method="hash",
            masked_column_add_method="inplace_substitute"
        )
        
        result = masking.applyPython(spark, df)
        rows = result.collect()
        
        # All columns should be hashed (integers)
        assert isinstance(rows[0].id, int)
        assert isinstance(rows[0].name, int)
        assert isinstance(rows[0].value, int)

