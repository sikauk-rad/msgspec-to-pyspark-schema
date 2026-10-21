from typing import TypedDict

from msgspec import Struct
from pyspark.sql import types as sqltypes


class MSGSpecToPySparkSchemaConfig(Struct, frozen = True, kw_only = True):
    integer_type: sqltypes.ShortType | sqltypes.IntegerType | sqltypes.LongType = sqltypes.LongType()
    float_type: sqltypes.FloatType | sqltypes.DoubleType = sqltypes.DoubleType()
    default_decimal_precision: int = 38
    default_decimal_scale: int = 18
    any_to_string: bool = False
    nonimplemented_to_string: bool = False


class MakeKwargs(TypedDict):
    seen: set[type]
    config: MSGSpecToPySparkSchemaConfig