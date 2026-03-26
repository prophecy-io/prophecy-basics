"""Shared PySpark helpers for prophecy_basics gems."""

from pyspark.sql.types import ByteType, DecimalType, DoubleType, FloatType
from pyspark.sql.types import IntegerType, LongType, ShortType, StructType


def dtype_for_column(schema: StructType, column_name: str):
    """Return the DataType of ``column_name`` in ``schema``, or None."""
    for f in schema.fields:
        if f.name == column_name:
            return f.dataType
    return None


def _is_integral_spark_type(dtype) -> bool:
    return isinstance(dtype, (ByteType, ShortType, IntegerType, LongType))


def _is_float_spark_type(dtype) -> bool:
    return isinstance(dtype, (FloatType, DoubleType, DecimalType))


def parse_literal_for_dtype(s: str, dtype):
    """Parse UI text to a Python value matching integral / float / other column types."""
    if s is None:
        return None
    text = str(s).strip()
    if dtype is None:
        try:
            return float(text)
        except (ValueError, TypeError):
            return None
    if _is_integral_spark_type(dtype):
        try:
            return int(float(text))
        except (ValueError, TypeError):
            return None
    if _is_float_spark_type(dtype):
        try:
            return float(text)
        except (ValueError, TypeError):
            return None
    return text


def coerce_repl_scalar(repl, dtype):
    """Coerce aggregate replacement values to int for integral Spark columns."""
    if repl is None or dtype is None:
        return repl
    if _is_integral_spark_type(dtype):
        try:
            if isinstance(repl, bool):
                return None
            return int(repl)
        except (ValueError, TypeError, OverflowError):
            return None
    return repl
