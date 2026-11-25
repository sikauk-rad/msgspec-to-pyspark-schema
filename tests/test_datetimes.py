"""Tests for the mapping of date-times onto Spark timestamp types."""

from datetime import datetime
from typing import Annotated

from msgspec import Meta, Struct
from pyspark.sql import types as sqltypes

from msgspec_to_pyspark_schema import (
    MSGSpecToPySparkSchemaConfig,
    convert_msgspec_struct_type_to_pyspark_schema,
)


class Timestamps(Struct, frozen = True, kw_only = True):

    aware: Annotated[datetime, Meta(tz = True)]
    naive: Annotated[datetime, Meta(tz = False)]
    unconstrained: datetime


def test_constrained_datetimes_follow_their_constraint() -> None:

    """Aware date-times become ``TIMESTAMP``, and naive ones ``TIMESTAMP_NTZ``."""

    schema = convert_msgspec_struct_type_to_pyspark_schema(Timestamps)

    assert isinstance(schema['aware'].dataType, sqltypes.TimestampType)
    assert isinstance(schema['naive'].dataType, sqltypes.TimestampNTZType)


def test_unconstrained_datetimes_follow_the_config() -> None:

    """Unconstrained date-times default to ``TIMESTAMP``, unless configured otherwise."""

    default_schema = convert_msgspec_struct_type_to_pyspark_schema(Timestamps)
    naive_schema = convert_msgspec_struct_type_to_pyspark_schema(
        Timestamps,
        config = MSGSpecToPySparkSchemaConfig(
            unconstrained_datetime_type = sqltypes.TimestampNTZType(),
        ),
    )

    assert isinstance(default_schema['unconstrained'].dataType, sqltypes.TimestampType)
    assert isinstance(naive_schema['unconstrained'].dataType, sqltypes.TimestampNTZType)