from collections.abc import Sequence

from msgspec import Struct
from pyspark.sql import types as sqltypes

type PySparkSchemaType = sqltypes.AtomicType | sqltypes.StructType | str | Sequence[str]

class MSGSpecToPySparkSchemaConfig(Struct):
    integer_type: sqltypes.IntegerType | sqltypes.LongType = sqltypes.IntegerType()
    float_type: sqltypes.FloatType | sqltypes.DoubleType = sqltypes.FloatType()
    default_decimal_precision: int = 38
    default_decimal_scale: int = 18
    any_to_string: bool = False
    nonimplemented_to_string: bool = False