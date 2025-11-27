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

DataEncoderDecoder = import_gem_class('DataEncoderDecoder')


def create_data_encoder_decoder_instance(
    column_names,
    enc_dec_method,
    new_column_add_method="inplace_substitute",
    enc_dec_charSet="UTF-8",
    prefix_suffix_option="Prefix",
    prefix_suffix_added="",
    aes_enc_dec_mode="GCM",
    aes_enc_dec_secretScope_key="",
    aes_enc_dec_secretKey_key="",
    aes_enc_dec_secretScope_aad="",
    aes_enc_dec_secretKey_aad="",
    aes_enc_dec_secretScope_iv="",
    aes_enc_dec_secretKey_iv=""
):
    """Helper function to create a DataEncoderDecoder instance with properties."""
    instance = DataEncoderDecoder()
    instance.props = Mock()
    instance.props.column_names = column_names
    instance.props.enc_dec_method = enc_dec_method
    instance.props.new_column_add_method = new_column_add_method
    instance.props.enc_dec_charSet = enc_dec_charSet
    instance.props.prefix_suffix_option = prefix_suffix_option
    instance.props.prefix_suffix_added = prefix_suffix_added
    instance.props.aes_enc_dec_mode = aes_enc_dec_mode
    instance.props.aes_enc_dec_secretScope_key = aes_enc_dec_secretScope_key
    instance.props.aes_enc_dec_secretKey_key = aes_enc_dec_secretKey_key
    instance.props.aes_enc_dec_secretScope_aad = aes_enc_dec_secretScope_aad
    instance.props.aes_enc_dec_secretKey_aad = aes_enc_dec_secretKey_aad
    instance.props.aes_enc_dec_secretScope_iv = aes_enc_dec_secretScope_iv
    instance.props.aes_enc_dec_secretKey_iv = aes_enc_dec_secretKey_iv
    return instance


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("test_data_encoder_decoder") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


class TestBase64Encoding:
    """Test cases for base64 encoding method."""
    
    def test_base64_encode_inplace(self, spark):
        """Test base64 encoding with inplace substitution."""
        data = [("Hello", "World", 1)]
        schema = StructType([
            StructField("text1", StringType(), True),
            StructField("text2", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text1", "text2"],
            enc_dec_method="base64",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 1
        # "Hello" in base64 is "SGVsbG8="
        assert rows[0].text1 == "SGVsbG8="
        # "World" in base64 is "V29ybGQ="
        assert rows[0].text2 == "V29ybGQ="
        assert rows[0].id == 1
    
    def test_base64_encode_with_prefix(self, spark):
        """Test base64 encoding with prefix."""
        data = [("Hello", 1)]
        schema = StructType([
            StructField("text", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="base64",
            new_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Prefix",
            prefix_suffix_added="encoded_"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # Original column should be preserved
        assert rows[0].text == "Hello"
        # New encoded column should be added
        assert rows[0].encoded_text == "SGVsbG8="
        assert rows[0].id == 1
    
    def test_base64_encode_with_suffix(self, spark):
        """Test base64 encoding with suffix."""
        data = [("Hello",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="base64",
            new_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Suffix",
            prefix_suffix_added="_encoded"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # Original column should be preserved
        assert rows[0].text == "Hello"
        # New encoded column should be added
        assert rows[0].text_encoded == "SGVsbG8="


class TestBase64Decoding:
    """Test cases for unbase64 decoding method."""
    
    def test_unbase64_decode_inplace(self, spark):
        """Test unbase64 decoding with inplace substitution."""
        data = [("SGVsbG8=", "V29ybGQ=", 1)]
        schema = StructType([
            StructField("encoded1", StringType(), True),
            StructField("encoded2", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        decoder = create_data_encoder_decoder_instance(
            column_names=["encoded1", "encoded2"],
            enc_dec_method="unbase64",
            new_column_add_method="inplace_substitute"
        )
        
        result = decoder.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 1
        assert rows[0].encoded1 == "Hello"
        assert rows[0].encoded2 == "World"
        assert rows[0].id == 1
    
    def test_base64_encode_decode_roundtrip(self, spark):
        """Test encoding and then decoding returns original value."""
        data = [("TestData123!@#",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        # Encode
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="base64",
            new_column_add_method="inplace_substitute"
        )
        encoded_df = encoder.applyPython(spark, df)
        
        # Decode
        decoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="unbase64",
            new_column_add_method="inplace_substitute"
        )
        decoded_df = decoder.applyPython(spark, encoded_df)
        
        rows = decoded_df.collect()
        assert rows[0].text == "TestData123!@#"


class TestHexEncoding:
    """Test cases for hex encoding method."""
    
    def test_hex_encode_inplace(self, spark):
        """Test hex encoding with inplace substitution."""
        data = [("AB", "CD", 1)]
        schema = StructType([
            StructField("text1", StringType(), True),
            StructField("text2", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text1", "text2"],
            enc_dec_method="hex",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 1
        # "AB" in hex is "4142"
        assert rows[0].text1 == "4142"
        # "CD" in hex is "4344"
        assert rows[0].text2 == "4344"
        assert rows[0].id == 1
    
    def test_hex_encode_with_prefix(self, spark):
        """Test hex encoding with prefix."""
        data = [("Test",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="hex",
            new_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Prefix",
            prefix_suffix_added="hex_"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # Original column should be preserved
        assert rows[0].text == "Test"
        # New hex encoded column should be added
        assert rows[0].hex_text == "54657374"


class TestHexDecoding:
    """Test cases for unhex decoding method."""
    
    def test_unhex_decode_inplace(self, spark):
        """Test unhex decoding with inplace substitution."""
        data = [("4142", "4344", 1)]
        schema = StructType([
            StructField("hex1", StringType(), True),
            StructField("hex2", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        decoder = create_data_encoder_decoder_instance(
            column_names=["hex1", "hex2"],
            enc_dec_method="unhex",
            new_column_add_method="inplace_substitute"
        )
        
        result = decoder.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 1
        assert rows[0].hex1 == "AB"
        assert rows[0].hex2 == "CD"
        assert rows[0].id == 1
    
    def test_hex_encode_decode_roundtrip(self, spark):
        """Test hex encoding and then decoding returns original value."""
        data = [("TestData",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        # Encode
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="hex",
            new_column_add_method="inplace_substitute"
        )
        encoded_df = encoder.applyPython(spark, df)
        
        # Decode
        decoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="unhex",
            new_column_add_method="inplace_substitute"
        )
        decoded_df = decoder.applyPython(spark, encoded_df)
        
        rows = decoded_df.collect()
        assert rows[0].text == "TestData"


class TestCharsetEncoding:
    """Test cases for encode method with charset."""
    
    def test_encode_utf8_inplace(self, spark):
        """Test encode with UTF-8 charset (inplace)."""
        data = [("Hello", 1)]
        schema = StructType([
            StructField("text", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="encode",
            enc_dec_charSet="UTF-8",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # encode with UTF-8 and then hex should produce hex representation
        assert len(rows) == 1
        assert rows[0].text == "48656C6C6F"  # "Hello" encoded with UTF-8 then hex
        assert rows[0].id == 1
    
    def test_encode_with_different_charsets(self, spark):
        """Test encoding with different charsets."""
        data = [("Test",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        for charset in ["UTF-8", "US-ASCII"]:
            encoder = create_data_encoder_decoder_instance(
                column_names=["text"],
                enc_dec_method="encode",
                enc_dec_charSet=charset,
                new_column_add_method="inplace_substitute"
            )
            
            result = encoder.applyPython(spark, df)
            rows = result.collect()
            
            # Should produce hex string
            assert len(rows[0].text) > 0
            assert all(c in "0123456789ABCDEF" for c in rows[0].text)


class TestCharsetDecoding:
    """Test cases for decode method with charset."""
    
    def test_decode_utf8_inplace(self, spark):
        """Test decode with UTF-8 charset (inplace)."""
        # "Hello" encoded with UTF-8 then hex is "48656C6C6F"
        data = [("48656C6C6F", 1)]
        schema = StructType([
            StructField("encoded", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        decoder = create_data_encoder_decoder_instance(
            column_names=["encoded"],
            enc_dec_method="decode",
            enc_dec_charSet="UTF-8",
            new_column_add_method="inplace_substitute"
        )
        
        result = decoder.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 1
        assert rows[0].encoded == "Hello"
        assert rows[0].id == 1
    
    def test_encode_decode_roundtrip_with_charset(self, spark):
        """Test encoding and decoding with charset returns original value."""
        data = [("TestData",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        # Encode
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="encode",
            enc_dec_charSet="UTF-8",
            new_column_add_method="inplace_substitute"
        )
        encoded_df = encoder.applyPython(spark, df)
        
        # Decode
        decoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="decode",
            enc_dec_charSet="UTF-8",
            new_column_add_method="inplace_substitute"
        )
        decoded_df = decoder.applyPython(spark, encoded_df)
        
        rows = decoded_df.collect()
        assert rows[0].text == "TestData"


class TestEdgeCases:
    """Test edge cases and special scenarios."""
    
    def test_encoding_with_null_values(self, spark):
        """Test encoding handles NULL values correctly."""
        data = [(None, "Hello", 1), ("World", None, 2)]
        schema = StructType([
            StructField("text1", StringType(), True),
            StructField("text2", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text1", "text2"],
            enc_dec_method="base64",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 2
        # NULL values should remain NULL
        assert rows[0].text1 is None
        assert rows[1].text2 is None
        # Non-NULL values should be encoded
        assert rows[0].text2 == "SGVsbG8="
        assert rows[1].text1 == "V29ybGQ="
    
    def test_encoding_multiple_columns(self, spark):
        """Test encoding multiple columns simultaneously."""
        data = [("Col1", "Col2", "Col3", 1)]
        schema = StructType([
            StructField("text1", StringType(), True),
            StructField("text2", StringType(), True),
            StructField("text3", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text1", "text2", "text3"],
            enc_dec_method="hex",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # All selected columns should be hex encoded
        assert rows[0].text1 == "436F6C31"
        assert rows[0].text2 == "436F6C32"
        assert rows[0].text3 == "436F6C33"
        # Unselected column should remain unchanged
        assert rows[0].id == 1
    
    def test_schema_preservation(self, spark):
        """Test that the DataFrame schema is preserved correctly."""
        data = [("Hello", "World", 1)]
        schema = StructType([
            StructField("text1", StringType(), True),
            StructField("text2", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text1"],
            enc_dec_method="base64",
            new_column_add_method="prefix_suffix_substitute",
            prefix_suffix_option="Prefix",
            prefix_suffix_added="encoded_"
        )
        
        result = encoder.applyPython(spark, df)
        
        # Check that all original columns are present
        assert "text1" in result.columns
        assert "text2" in result.columns
        assert "id" in result.columns
        # Check that new column is added
        assert "encoded_text1" in result.columns
    
    def test_empty_dataframe(self, spark):
        """Test encoding on an empty DataFrame."""
        schema = StructType([
            StructField("text", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame([], schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="base64",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        assert len(rows) == 0
        assert "text" in result.columns
        assert "id" in result.columns
    
    def test_special_characters_encoding(self, spark):
        """Test encoding special characters."""
        data = [("Hello@World#2024!",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="base64",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # Should successfully encode special characters
        assert len(rows[0].text) > 0
        # Decode to verify
        decoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="unbase64",
            new_column_add_method="inplace_substitute"
        )
        decoded_result = decoder.applyPython(spark, result)
        decoded_rows = decoded_result.collect()
        assert decoded_rows[0].text == "Hello@World#2024!"
    
    def test_unicode_characters_encoding(self, spark):
        """Test encoding unicode characters."""
        data = [("Hello世界",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="base64",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # Should successfully encode unicode
        assert len(rows[0].text) > 0
        
        # Decode to verify
        decoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="unbase64",
            new_column_add_method="inplace_substitute"
        )
        decoded_result = decoder.applyPython(spark, result)
        decoded_rows = decoded_result.collect()
        assert decoded_rows[0].text == "Hello世界"
    
    def test_empty_string_encoding(self, spark):
        """Test encoding empty strings."""
        data = [("",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="base64",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # Empty string should produce empty base64 string
        assert rows[0].text == ""
    
    def test_column_order_preservation(self, spark):
        """Test that column order is preserved."""
        data = [("A", "B", "C", 1)]
        schema = StructType([
            StructField("col1", StringType(), True),
            StructField("col2", StringType(), True),
            StructField("col3", StringType(), True),
            StructField("id", IntegerType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["col2"],
            enc_dec_method="base64",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        
        # Column order should be preserved
        assert result.columns == ["col1", "col2", "col3", "id"]
    
    def test_consistency_same_input_same_output(self, spark):
        """Test that encoding the same value produces consistent results."""
        data = [("Test",), ("Different",), ("Test",)]
        schema = StructType([StructField("text", StringType(), True)])
        df = spark.createDataFrame(data, schema)
        
        encoder = create_data_encoder_decoder_instance(
            column_names=["text"],
            enc_dec_method="hex",
            new_column_add_method="inplace_substitute"
        )
        
        result = encoder.applyPython(spark, df)
        rows = result.collect()
        
        # Same input should produce same output
        assert rows[0].text == rows[2].text
        # Different input should produce different output
        assert rows[0].text != rows[1].text

