import msgspec.inspect as msginspect
from pyspark.sql import types as sqltypes

JSON_INCOMPATIBLE_CHARS = '[^A-Za-z0-9_]+'

MSGSPEC_TYPE_TO_SPARK_MAP = {
    msginspect.BoolType: sqltypes.BooleanType(),
    msginspect.StrType: sqltypes.StringType(),
    msginspect.BytesType: sqltypes.BinaryType(),
    msginspect.ByteArrayType: sqltypes.BinaryType(),
    msginspect.NoneType: sqltypes.NullType(),
    msginspect.UUIDType: sqltypes.StringType(),
    msginspect.DateType: sqltypes.DateType(),
    msginspect.DateTimeType: sqltypes.TimestampType(),
    msginspect.TimeType: sqltypes.TimeType(),
    msginspect.TimeDeltaType: sqltypes.DayTimeIntervalType(),
}