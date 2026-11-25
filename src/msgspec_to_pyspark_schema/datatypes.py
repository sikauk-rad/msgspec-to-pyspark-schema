from typing import TypedDict

from msgspec import Struct
from pyspark.sql import types as sqltypes


class MSGSpecToPySparkSchemaConfig(Struct, frozen = True, kw_only = True):

    """
    Configuration of the msgspec to PySpark type mapping.

    Parameters
    ----------
    integer_type : ShortType | IntegerType | LongType, default=LongType()
        Spark type of ``int`` values.
    float_type : FloatType | DoubleType, default=DoubleType()
        Spark type of ``float`` values.
    default_decimal_precision : int, default=38
        Precision of ``Decimal`` values, which msgspec does not expose.
    default_decimal_scale : int, default=18
        Scale of ``Decimal`` values, which msgspec does not expose.
    any_to_string : bool, default=False
        Whether ``Any`` maps to ``StringType`` rather than raising.
    nonimplemented_to_string : bool, default=False
        Whether unsupported types map to ``StringType`` rather than raising.
    unconstrained_datetime_type : TimestampType | TimestampNTZType, default=TimestampType()
        Spark type of ``datetime`` values carrying no ``Meta(tz = ...)`` constraint, which
        may be either timezone-aware or naive. ``TimestampType`` accepts both, reading
        naive values in the Spark session time zone. ``TimestampNTZType`` accepts naive
        values only. Constrained values ignore this setting: ``Meta(tz = True)`` always 
        maps to ``TimestampType``, and ``Meta(tz = False)`` to ``TimestampNTZType``.
    """

    integer_type: sqltypes.ShortType | sqltypes.IntegerType | sqltypes.LongType = sqltypes.LongType()
    float_type: sqltypes.FloatType | sqltypes.DoubleType = sqltypes.DoubleType()
    default_decimal_precision: int = 38
    default_decimal_scale: int = 18
    any_to_string: bool = False
    nonimplemented_to_string: bool = False
    unconstrained_datetime_type: sqltypes.TimestampType | sqltypes.TimestampNTZType = sqltypes.TimestampType()


class MakeKwargs(TypedDict):
    seen: set[type]
    config: MSGSpecToPySparkSchemaConfig