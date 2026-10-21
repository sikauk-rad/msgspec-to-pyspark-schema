import msgspec.inspect as msginspect
from pyspark.sql import types as sqltypes

JSON_INCOMPATIBLE_CHARS = '[^A-Za-z0-9_]+'

MSGSPEC_TYPE_TO_SPARK_MAP = {
    msginspect.BoolType: sqltypes.BooleanType(),
    msginspect.StrType: sqltypes.StringType(),
    msginspect.BytesType: sqltypes.BinaryType(),
    msginspect.ByteArrayType: sqltypes.BinaryType(),
    msginspect.UUIDType: sqltypes.StringType(),
    msginspect.DateType: sqltypes.DateType(),
    msginspect.TimeType: sqltypes.TimeType(),
    msginspect.TimeDeltaType: sqltypes.DayTimeIntervalType(),
}

RECORD_TYPES = (
    msginspect.StructType,
    msginspect.DataclassType,
    msginspect.TypedDictType,
    msginspect.NamedTupleType,
)