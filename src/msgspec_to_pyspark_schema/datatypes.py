from collections.abc import Sequence
from pyspark.sql import types as sqltypes

type PySparkSchemaType = sqltypes.AtomicType | sqltypes.StructType | str | Sequence[str]