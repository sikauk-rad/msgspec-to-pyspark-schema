from dataclasses import dataclass
import datetime
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from typing import Annotated, Any, Literal, NamedTuple, TypedDict
from uuid import UUID

from msgspec import Meta, Struct, field
import msgspec.inspect as msginspect
from pyspark.sql import types as sqltypes
import pytest

from msgspec_to_pyspark_schema import (
    MSGSpecToPySparkSchemaConfig,
    convert_msgspec_struct_type_to_pyspark_schema,
)
from msgspec_to_pyspark_schema.exceptions import (
    MSGSpecToPySparkNotImplementedError,
    MSGSpecToPySparkTypeError,
)
from msgspec_to_pyspark_schema.msgspec_to_pyspark_schema import (
    make_spark_datatype_from_msgspec_type,
    make_spark_struct_from_msgspec_struct_type,
)


DEFAULT_CONFIG = MSGSpecToPySparkSchemaConfig()


# --------------------------------------------------------------------------------------
# Test types (module level so that forward references resolve)
# --------------------------------------------------------------------------------------

class Address(Struct):
    city: str
    postcode: str | None


class Person(Struct):
    name: str
    age: int
    home: Address
    work: Address | None = None


class WithDefault(Struct):
    count: int = 0


class Node(Struct):
    value: int
    child: 'Node | None' = None


class TreeA(Struct):
    b: 'TreeB | None' = None


class TreeB(Struct):
    a: TreeA | None = None


class Cat(Struct, tag = True):
    name: str


class Dog(Struct, tag_field = 'kind', tag = 1):
    name: str


class Renamed(Struct, rename = 'camel'):
    first_name: str
    last_name: str = field(name = 'surname')


@dataclass
class DataclassRecord:
    a: int
    b: str | None = None


class TypedDictRecord(TypedDict):
    a: int
    b: str


class PartialTypedDictRecord(TypedDict, total = False):
    a: int


class NamedTupleRecord(NamedTuple):
    a: int
    b: str = 'x'


class RecordContainer(Struct):
    dataclass_record: DataclassRecord
    typed_dict_record: TypedDictRecord
    partial_typed_dict_record: PartialTypedDictRecord
    named_tuple_record: NamedTupleRecord


class WithAny(Struct):
    data: Any


class Colour(StrEnum):
    RED = 'red'
    GREEN = 'green'


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


class MixedEnum(Enum):
    NUMBER = 1
    WORD = 'word'


ADDRESS_SCHEMA = sqltypes.StructType(
    [
        sqltypes.StructField('city', sqltypes.StringType(), False),
        sqltypes.StructField('postcode', sqltypes.StringType(), True),
    ]
)


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def to_spark(
    python_type: Any,
    config: MSGSpecToPySparkSchemaConfig = DEFAULT_CONFIG,
) -> tuple[sqltypes.DataType, bool]:


    """
    Inspect a Python type with msgspec and convert it into a Spark data type.
    """


    return make_spark_datatype_from_msgspec_type(
        msgspec_type = msginspect.type_info(python_type),
        seen = set(),
        config = config,
    )


def inspected_to_spark(
    msgspec_type: msginspect.Type,
    config: MSGSpecToPySparkSchemaConfig = DEFAULT_CONFIG,
) -> tuple[sqltypes.DataType, bool]:


    """
    Convert an already constructed msgspec inspection type into a Spark data type.

    Used for edge cases that are awkward or impossible to produce via
    ``msgspec.inspect.type_info``.
    """


    return make_spark_datatype_from_msgspec_type(
        msgspec_type = msgspec_type,
        seen = set(),
        config = config,
    )


# --------------------------------------------------------------------------------------
# Scalar types
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    ('python_type', 'expected'),
    [
        (bool, sqltypes.BooleanType()),
        (int, sqltypes.LongType()),
        (float, sqltypes.DoubleType()),
        (str, sqltypes.StringType()),
        (bytes, sqltypes.BinaryType()),
        (bytearray, sqltypes.BinaryType()),
        (UUID, sqltypes.StringType()),
        (datetime.date, sqltypes.DateType()),
        (datetime.time, sqltypes.TimeType()),
        (datetime.timedelta, sqltypes.DayTimeIntervalType()),
        (datetime.datetime, sqltypes.TimestampNTZType()),
        (Annotated[datetime.datetime, Meta(tz = False)], sqltypes.TimestampNTZType()),
        (Annotated[datetime.datetime, Meta(tz = True)], sqltypes.TimestampType()),
        (Decimal, sqltypes.DecimalType(precision = 38, scale = 18)),
    ],
)
def test_scalar_types(python_type: Any, expected: sqltypes.DataType) -> None:
    assert to_spark(python_type) == (expected, False)


def test_none_type_is_null_and_nullable() -> None:
    assert to_spark(None) == (sqltypes.NullType(), True)


@pytest.mark.parametrize(
    ('python_type', 'expected'),
    [
        (Annotated[int, Meta(ge = 0)], sqltypes.LongType()),
        (Annotated[str, Meta(max_length = 10)], sqltypes.StringType()),
        (Annotated[str, Meta(description = 'A described string.')], sqltypes.StringType()),
    ],
)
def test_constraints_and_annotations_do_not_affect_type(
    python_type: Any,
    expected: sqltypes.DataType,
) -> None:
    assert to_spark(python_type) == (expected, False)


def test_metadata_is_unwrapped() -> None:
    metadata = msginspect.Metadata(
        type = msginspect.IntType(),
        extra = {'unit': 'kg'},
    )
    assert inspected_to_spark(metadata) == (sqltypes.LongType(), False)


def test_config_controls_numeric_types() -> None:
    config = MSGSpecToPySparkSchemaConfig(
        integer_type = sqltypes.IntegerType(),
        float_type = sqltypes.FloatType(),
        default_decimal_precision = 10,
        default_decimal_scale = 2,
    )

    assert to_spark(int, config) == (sqltypes.IntegerType(), False)
    assert to_spark(float, config) == (sqltypes.FloatType(), False)
    assert to_spark(Decimal, config) == (
        sqltypes.DecimalType(precision = 10, scale = 2), False,
    )


# --------------------------------------------------------------------------------------
# Unions
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    ('python_type', 'expected'),
    [
        (int | None, sqltypes.LongType()),
        (str | None, sqltypes.StringType()),
        (list[int] | None, sqltypes.ArrayType(sqltypes.LongType(), False)),
        (Address | None, ADDRESS_SCHEMA),
    ],
)
def test_optional_is_nullable_base_type(
    python_type: Any,
    expected: sqltypes.DataType,
) -> None:
    assert to_spark(python_type) == (expected, True)


@pytest.mark.parametrize(
    'python_type',
    [
        int | str,
        int | str | None,
        Cat | Dog,
    ],
)
def test_general_unions_are_rejected(python_type: Any) -> None:
    with pytest.raises(MSGSpecToPySparkTypeError, match = 'Unsupported union type'):
        to_spark(python_type)


def test_union_of_only_none_is_null() -> None:
    union = msginspect.UnionType(types = (msginspect.NoneType(),))
    assert inspected_to_spark(union) == (sqltypes.NullType(), True)


# --------------------------------------------------------------------------------------
# Collections, mappings and tuples
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    ('python_type', 'expected'),
    [
        (list[int], sqltypes.ArrayType(sqltypes.LongType(), False)),
        (set[str], sqltypes.ArrayType(sqltypes.StringType(), False)),
        (frozenset[int], sqltypes.ArrayType(sqltypes.LongType(), False)),
        (tuple[int, ...], sqltypes.ArrayType(sqltypes.LongType(), False)),
        (list[int | None], sqltypes.ArrayType(sqltypes.LongType(), True)),
        (
            list[list[str]],
            sqltypes.ArrayType(sqltypes.ArrayType(sqltypes.StringType(), False), False),
        ),
        (list[Address], sqltypes.ArrayType(ADDRESS_SCHEMA, False)),
    ],
)
def test_collections_become_arrays(
    python_type: Any,
    expected: sqltypes.DataType,
) -> None:
    assert to_spark(python_type) == (expected, False)


@pytest.mark.parametrize(
    ('python_type', 'expected'),
    [
        (
            dict[str, int],
            sqltypes.MapType(sqltypes.StringType(), sqltypes.LongType(), False),
        ),
        (
            dict[int, str | None],
            sqltypes.MapType(sqltypes.LongType(), sqltypes.StringType(), True),
        ),
        (
            dict[str, list[float]],
            sqltypes.MapType(
                sqltypes.StringType(),
                sqltypes.ArrayType(sqltypes.DoubleType(), False),
                False,
            ),
        ),
    ],
)
def test_dicts_become_maps(python_type: Any, expected: sqltypes.DataType) -> None:
    assert to_spark(python_type) == (expected, False)


@pytest.mark.parametrize(
    'key_type',
    [
        msginspect.UnionType(types = (msginspect.StrType(), msginspect.NoneType())),
        msginspect.NoneType(),
    ],
)
def test_maps_with_nullable_keys_are_rejected(key_type: msginspect.Type) -> None:
    dict_type = msginspect.DictType(
        key_type = key_type,
        value_type = msginspect.IntType(),
    )
    with pytest.raises(MSGSpecToPySparkTypeError, match = 'nullable/None keys'):
        inspected_to_spark(dict_type)


def test_fixed_tuples_become_positional_structs() -> None:
    expected = sqltypes.StructType(
        [
            sqltypes.StructField('_1', sqltypes.LongType(), False),
            sqltypes.StructField('_2', sqltypes.StringType(), True),
            sqltypes.StructField('_3', ADDRESS_SCHEMA, False),
        ]
    )
    assert to_spark(tuple[int, str | None, Address]) == (expected, False)


# --------------------------------------------------------------------------------------
# Literals and enums
# --------------------------------------------------------------------------------------

def test_literal_via_inspection() -> None:
    assert to_spark(Literal['a', 'b']) == (sqltypes.StringType(), False)
    assert to_spark(Literal[1, 2]) == (sqltypes.LongType(), False)


@pytest.mark.parametrize(
    ('values', 'expected'),
    [
        (('a', 'b'), (sqltypes.StringType(), False)),
        (('a', None), (sqltypes.StringType(), True)),
        ((1, 2), (sqltypes.LongType(), False)),
        ((1, None), (sqltypes.LongType(), True)),
        ((True, False), (sqltypes.BooleanType(), False)),
        ((1, 2.5), (sqltypes.DoubleType(), False)),
        ((None,), (sqltypes.NullType(), True)),
    ],
)
def test_literal_narrowest_type(
    values: tuple[Any, ...],
    expected: tuple[sqltypes.DataType, bool],
) -> None:
    literal = msginspect.LiteralType(values = values)
    assert inspected_to_spark(literal) == expected


@pytest.mark.parametrize(
    'values',
    [
        ('a', 1),
        (True, 1),
        ('a', 1.5),
    ],
)
def test_mixed_literals_are_rejected(values: tuple[Any, ...]) -> None:
    literal = msginspect.LiteralType(values = values)
    with pytest.raises(MSGSpecToPySparkTypeError, match = 'Mixed-type Literal'):
        inspected_to_spark(literal)


def test_literal_respects_integer_config() -> None:
    config = MSGSpecToPySparkSchemaConfig(integer_type = sqltypes.ShortType())
    assert to_spark(Literal[1, 2], config) == (sqltypes.ShortType(), False)


def test_str_enum_is_string() -> None:
    assert to_spark(Colour) == (sqltypes.StringType(), False)


def test_int_enum_uses_configured_integer_type() -> None:
    assert to_spark(Priority) == (sqltypes.LongType(), False)

    config = MSGSpecToPySparkSchemaConfig(integer_type = sqltypes.IntegerType())
    assert to_spark(Priority, config) == (sqltypes.IntegerType(), False)


def test_mixed_enum_is_rejected() -> None:
    enum_type = msginspect.EnumType(cls = MixedEnum)
    with pytest.raises(MSGSpecToPySparkTypeError, match = 'Mixed-type enum'):
        inspected_to_spark(enum_type)


# --------------------------------------------------------------------------------------
# Any and unsupported types
# --------------------------------------------------------------------------------------

def test_any_is_rejected_by_default() -> None:
    with pytest.raises(MSGSpecToPySparkTypeError, match = '`Any` type not supported'):
        to_spark(Any)


def test_any_to_string() -> None:
    config = MSGSpecToPySparkSchemaConfig(any_to_string = True)
    assert to_spark(Any, config) == (sqltypes.StringType(), True)


def test_any_is_not_covered_by_nonimplemented_to_string() -> None:
    config = MSGSpecToPySparkSchemaConfig(nonimplemented_to_string = True)
    with pytest.raises(MSGSpecToPySparkTypeError):
        to_spark(Any, config)


UNSUPPORTED_TYPES = [
    msginspect.RawType(),
    msginspect.ExtType(),
    msginspect.MemoryViewType(),
]


@pytest.mark.parametrize('msgspec_type', UNSUPPORTED_TYPES)
def test_unsupported_types_raise(msgspec_type: msginspect.Type) -> None:
    with pytest.raises(MSGSpecToPySparkNotImplementedError, match = 'Unsupported type'):
        inspected_to_spark(msgspec_type)


@pytest.mark.parametrize('msgspec_type', UNSUPPORTED_TYPES)
def test_unsupported_types_to_string(msgspec_type: msginspect.Type) -> None:
    config = MSGSpecToPySparkSchemaConfig(nonimplemented_to_string = True)
    assert inspected_to_spark(msgspec_type, config) == (sqltypes.StringType(), True)


# --------------------------------------------------------------------------------------
# Struct schemas
# --------------------------------------------------------------------------------------

def test_flat_struct() -> None:
    assert convert_msgspec_struct_type_to_pyspark_schema(Address) == ADDRESS_SCHEMA


def test_nested_struct_and_repeated_struct_is_not_recursion() -> None:
    expected = sqltypes.StructType(
        [
            sqltypes.StructField('name', sqltypes.StringType(), False),
            sqltypes.StructField('age', sqltypes.LongType(), False),
            sqltypes.StructField('home', ADDRESS_SCHEMA, False),
            sqltypes.StructField('work', ADDRESS_SCHEMA, True),
        ]
    )
    assert convert_msgspec_struct_type_to_pyspark_schema(Person) == expected


def test_field_order_is_preserved() -> None:
    schema = convert_msgspec_struct_type_to_pyspark_schema(Person)
    assert schema.fieldNames() == ['name', 'age', 'home', 'work']


def test_non_required_field_is_nullable_even_when_type_is_not() -> None:
    expected = sqltypes.StructType(
        [sqltypes.StructField('count', sqltypes.LongType(), True)]
    )
    assert convert_msgspec_struct_type_to_pyspark_schema(WithDefault) == expected


def test_renamed_fields_use_encode_name() -> None:
    schema = convert_msgspec_struct_type_to_pyspark_schema(Renamed)
    assert schema.fieldNames() == ['firstName', 'surname']


def test_string_tagged_struct() -> None:
    expected = sqltypes.StructType(
        [
            sqltypes.StructField('type', sqltypes.StringType(), False),
            sqltypes.StructField('name', sqltypes.StringType(), False),
        ]
    )
    assert convert_msgspec_struct_type_to_pyspark_schema(Cat) == expected


def test_integer_tagged_struct_uses_configured_integer_type() -> None:
    config = MSGSpecToPySparkSchemaConfig(integer_type = sqltypes.IntegerType())
    expected = sqltypes.StructType(
        [
            sqltypes.StructField('kind', sqltypes.IntegerType(), False),
            sqltypes.StructField('name', sqltypes.StringType(), False),
        ]
    )
    assert convert_msgspec_struct_type_to_pyspark_schema(
        Dog,
        config = config,
    ) == expected


def test_other_record_types_are_nested_as_structs() -> None:
    expected = sqltypes.StructType(
        [
            sqltypes.StructField(
                'dataclass_record',
                sqltypes.StructType(
                    [
                        sqltypes.StructField('a', sqltypes.LongType(), False),
                        sqltypes.StructField('b', sqltypes.StringType(), True),
                    ]
                ),
                False,
            ),
            sqltypes.StructField(
                'typed_dict_record',
                sqltypes.StructType(
                    [
                        sqltypes.StructField('a', sqltypes.LongType(), False),
                        sqltypes.StructField('b', sqltypes.StringType(), False),
                    ]
                ),
                False,
            ),
            sqltypes.StructField(
                'partial_typed_dict_record',
                sqltypes.StructType(
                    [sqltypes.StructField('a', sqltypes.LongType(), True)]
                ),
                False,
            ),
            sqltypes.StructField(
                'named_tuple_record',
                sqltypes.StructType(
                    [
                        sqltypes.StructField('a', sqltypes.LongType(), False),
                        sqltypes.StructField('b', sqltypes.StringType(), True),
                    ]
                ),
                False,
            ),
        ]
    )
    assert convert_msgspec_struct_type_to_pyspark_schema(RecordContainer) == expected


def test_any_field_follows_config() -> None:
    with pytest.raises(MSGSpecToPySparkTypeError):
        convert_msgspec_struct_type_to_pyspark_schema(WithAny)

    schema = convert_msgspec_struct_type_to_pyspark_schema(
        WithAny,
        config = MSGSpecToPySparkSchemaConfig(any_to_string = True),
    )
    assert schema == sqltypes.StructType(
        [sqltypes.StructField('data', sqltypes.StringType(), True)]
    )


@pytest.mark.parametrize('struct', [Node, TreeA, TreeB])
def test_recursive_structs_are_rejected(struct: type[Struct]) -> None:
    with pytest.raises(MSGSpecToPySparkTypeError, match = 'Recursive type'):
        convert_msgspec_struct_type_to_pyspark_schema(struct)


@pytest.mark.parametrize('not_a_struct', [DataclassRecord, int, list[int]])
def test_non_struct_input_is_rejected(not_a_struct: Any) -> None:
    with pytest.raises(MSGSpecToPySparkTypeError, match = 'Expected a msgspec StructType'):
        convert_msgspec_struct_type_to_pyspark_schema(not_a_struct)


# --------------------------------------------------------------------------------------
# Recursion tracking state
# --------------------------------------------------------------------------------------

def test_seen_is_restored_after_success() -> None:
    seen: set[type] = set()
    make_spark_struct_from_msgspec_struct_type(
        struct_type = msginspect.type_info(Person),
        seen = seen,
        config = DEFAULT_CONFIG,
    )
    assert seen == set()


def test_seen_is_restored_after_failure() -> None:
    seen: set[type] = set()
    with pytest.raises(MSGSpecToPySparkTypeError):
        make_spark_struct_from_msgspec_struct_type(
            struct_type = msginspect.type_info(Node),
            seen = seen,
            config = DEFAULT_CONFIG,
        )
    assert seen == set()


# --------------------------------------------------------------------------------------
# Docstring examples
# --------------------------------------------------------------------------------------

def test_docstring_examples() -> None:
    assert make_spark_datatype_from_msgspec_type(
        msginspect.type_info(int | None),
        seen = set(),
    ) == (sqltypes.LongType(), True)

    assert make_spark_datatype_from_msgspec_type(
        msginspect.type_info(list[str]),
        seen = set(),
    ) == (sqltypes.ArrayType(sqltypes.StringType(), False), False)
