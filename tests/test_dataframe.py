import datetime
from decimal import Decimal
from enum import IntEnum, StrEnum
from uuid import UUID

from msgspec import Struct, field
from pyspark.sql import SparkSession
from pyspark.sql import types as sqltypes
import pytest

from msgspec_to_pyspark_schema import (
    MSGSpecToPySparkSchemaConfig,
    convert_msgspec_struct_type_to_pyspark_schema,
    convert_msgspec_structs_to_pyspark_df,
)
from msgspec_to_pyspark_schema.exceptions import (
    MSGSpecToPySparkTypeError,
    MSGSpecToPySparkValueError,
)
from msgspec_to_pyspark_schema.utilities import struct_to_dict


class User(Struct):
    id: int
    name: str
    email: str | None = None


class Admin(Struct):
    id: int
    name: str


class Address(Struct):
    city: str
    postcode: str | None


class Cat(Struct, tag = True):
    name: str


class Renamed(Struct, rename = 'camel'):
    first_name: str
    last_name: str = field(name = 'surname')


class Colour(StrEnum):
    RED = 'red'


class Priority(IntEnum):
    HIGH = 2


class Rich(Struct):
    flag: bool
    count: int
    ratio: float
    label: str
    payload: bytes
    identifier: UUID
    day: datetime.date
    moment: datetime.datetime
    duration: datetime.timedelta
    amount: Decimal
    tags: list[str]
    scores: dict[str, int]
    pair: tuple[int, str]
    colour: Colour
    priority: Priority
    address: Address
    note: str | None = None


IDENTIFIER = UUID('12345678-1234-5678-1234-567812345678')


# --------------------------------------------------------------------------------------
# struct_to_dict
# --------------------------------------------------------------------------------------

def test_struct_to_dict_preserves_spark_native_types() -> None:
    rich = Rich(
        flag = True,
        count = 3,
        ratio = 0.5,
        label = 'label',
        payload = b'\x00\x01',
        identifier = IDENTIFIER,
        day = datetime.date(2024, 1, 2),
        moment = datetime.datetime(2024, 1, 2, 3, 4, 5),
        duration = datetime.timedelta(hours = 1),
        amount = Decimal('12.5'),
        tags = ['a', 'b'],
        scores = {'a': 1},
        pair = (1, 'x'),
        colour = Colour.RED,
        priority = Priority.HIGH,
        address = Address(city = 'Leeds', postcode = None),
    )
    as_dict = struct_to_dict(rich)

    assert as_dict['payload'] == b'\x00\x01'
    assert as_dict['identifier'] == str(IDENTIFIER)
    assert as_dict['day'] == datetime.date(2024, 1, 2)
    assert as_dict['moment'] == datetime.datetime(2024, 1, 2, 3, 4, 5)
    assert as_dict['duration'] == datetime.timedelta(hours = 1)
    assert as_dict['amount'] == Decimal('12.5')
    assert as_dict['colour'] == 'red'
    assert as_dict['priority'] == 2
    assert as_dict['address'] == {'city': 'Leeds', 'postcode': None}


def test_struct_to_dict_handles_sequences() -> None:
    users = [User(id = 1, name = 'Alice'), User(id = 2, name = 'Bob')]
    assert struct_to_dict(users) == [
        {'id': 1, 'name': 'Alice', 'email': None},
        {'id': 2, 'name': 'Bob', 'email': None},
    ]


# --------------------------------------------------------------------------------------
# DataFrame creation
# --------------------------------------------------------------------------------------

def test_schema_is_inferred(spark: SparkSession) -> None:
    users = [
        User(id = 1, name = 'Alice', email = 'alice@example.com'),
        User(id = 2, name = 'Bob'),
    ]
    df = convert_msgspec_structs_to_pyspark_df(users, spark_session = spark)

    assert df.schema == convert_msgspec_struct_type_to_pyspark_schema(User)
    assert [row.asDict() for row in df.orderBy('id').collect()] == [
        {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
        {'id': 2, 'name': 'Bob', 'email': None},
    ]


def test_config_is_applied_during_inference(spark: SparkSession) -> None:
    df = convert_msgspec_structs_to_pyspark_df(
        [User(id = 1, name = 'Alice')],
        config = MSGSpecToPySparkSchemaConfig(integer_type = sqltypes.IntegerType()),
        spark_session = spark,
    )
    assert df.schema['id'].dataType == sqltypes.IntegerType()


def test_explicit_schema_takes_precedence(spark: SparkSession) -> None:
    schema = sqltypes.StructType(
        [
            sqltypes.StructField('id', sqltypes.IntegerType(), False),
            sqltypes.StructField('name', sqltypes.StringType(), True),
            sqltypes.StructField('email', sqltypes.StringType(), True),
        ]
    )
    df = convert_msgspec_structs_to_pyspark_df(
        [User(id = 1, name = 'Alice')],
        schema = schema,
        config = MSGSpecToPySparkSchemaConfig(integer_type = sqltypes.ShortType()),
        spark_session = spark,
    )
    assert df.schema == schema


def test_empty_without_schema_raises(spark: SparkSession) -> None:
    with pytest.raises(MSGSpecToPySparkValueError, match = 'schema must be defined'):
        convert_msgspec_structs_to_pyspark_df([], spark_session = spark)


def test_empty_with_schema_returns_empty_dataframe(spark: SparkSession) -> None:
    schema = convert_msgspec_struct_type_to_pyspark_schema(User)
    df = convert_msgspec_structs_to_pyspark_df(
        [],
        schema = schema,
        spark_session = spark,
    )
    assert df.schema == schema
    assert df.count() == 0


def test_verify_types_rejects_mixed_structs(spark: SparkSession) -> None:
    with pytest.raises(MSGSpecToPySparkTypeError, match = 'not consistent'):
        convert_msgspec_structs_to_pyspark_df(
            [User(id = 1, name = 'Alice'), Admin(id = 2, name = 'Bob')],
            verify_types = True,
            spark_session = spark,
        )


def test_verify_types_accepts_uniform_structs(spark: SparkSession) -> None:
    df = convert_msgspec_structs_to_pyspark_df(
        [User(id = 1, name = 'Alice'), User(id = 2, name = 'Bob')],
        verify_types = True,
        spark_session = spark,
    )
    assert df.count() == 2


def test_supplied_session_is_used(spark: SparkSession) -> None:
    df = convert_msgspec_structs_to_pyspark_df(
        [User(id = 1, name = 'Alice')],
        spark_session = spark,
    )
    assert df.sparkSession is spark


def test_tagged_struct_round_trip(spark: SparkSession) -> None:
    df = convert_msgspec_structs_to_pyspark_df(
        [Cat(name = 'Tom')],
        spark_session = spark,
    )
    assert df.collect()[0].asDict() == {'type': 'Cat', 'name': 'Tom'}


def test_renamed_struct_round_trip(spark: SparkSession) -> None:
    df = convert_msgspec_structs_to_pyspark_df(
        [Renamed(first_name = 'Ada', last_name = 'Lovelace')],
        spark_session = spark,
    )
    assert df.columns == ['firstName', 'surname']
    assert df.collect()[0].asDict() == {'firstName': 'Ada', 'surname': 'Lovelace'}


def test_rich_struct_round_trip(spark: SparkSession) -> None:
    rich = Rich(
        flag = True,
        count = 3,
        ratio = 0.5,
        label = 'label',
        payload = b'\x00\x01',
        identifier = IDENTIFIER,
        day = datetime.date(2024, 1, 2),
        moment = datetime.datetime(2024, 1, 2, 3, 4, 5),
        duration = datetime.timedelta(hours = 1, seconds = 30),
        amount = Decimal('12.5'),
        tags = ['a', 'b'],
        scores = {'a': 1, 'b': 2},
        pair = (1, 'x'),
        colour = Colour.RED,
        priority = Priority.HIGH,
        address = Address(city = 'Leeds', postcode = None),
    )
    row = convert_msgspec_structs_to_pyspark_df(
        [rich],
        spark_session = spark,
    ).collect()[0]

    assert row.flag is True
    assert row['count'] == 3
    assert row.ratio == 0.5
    assert row.label == 'label'
    assert bytes(row.payload) == b'\x00\x01'
    assert row.identifier == str(IDENTIFIER)
    assert row.day == datetime.date(2024, 1, 2)
    assert row.moment == datetime.datetime(2024, 1, 2, 3, 4, 5)
    assert row.duration == datetime.timedelta(hours = 1, seconds = 30)
    assert row.amount == Decimal('12.5')
    assert row.tags == ['a', 'b']
    assert row.scores == {'a': 1, 'b': 2}
    assert row.pair.asDict() == {'_1': 1, '_2': 'x'}
    assert row.colour == 'red'
    assert row.priority == 2
    assert row.address.asDict() == {'city': 'Leeds', 'postcode': None}
    assert row.note is None