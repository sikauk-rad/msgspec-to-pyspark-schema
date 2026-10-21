# msgspec_to_pyspark_schema

Small utility package that converts [msgspec](https://jcristharif.com/msgspec/) `Struct`
schemas into PySpark schemas, and msgspec `Struct` instances into PySpark DataFrames.

The schema of a dataset only has to be defined once, as a msgspec `Struct`. It can then
be used for validation and (de)serialisation in Python and as the schema of a Spark
DataFrame, without the two drifting apart.

## Contents

- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Converting schemas](#converting-schemas)
- [Creating DataFrames](#creating-dataframes)
- [Configuration](#configuration)
- [Type mapping](#type-mapping)
- [Nullability](#nullability)
- [Field names, tags and record types](#field-names-tags-and-record-types)
- [Unsupported types and fallbacks](#unsupported-types-and-fallbacks)
- [String normalisation utilities](#string-normalisation-utilities)
- [Exceptions](#exceptions)
- [Limitations](#limitations)
- [Development](#development)
- [Licence](#licence)

## Requirements

- Python 3.12 or later
- `msgspec >= 0.21.1`
- `pyspark >= 4.1.0` (required for `TimeType` support)
- A Java runtime supported by your Spark version, if you create DataFrames or run the
  Spark-dependent tests

## Installation

Install directly from GitHub:

```bash
pip install "msgspec_to_pyspark_schema @ git+https://github.com/sikauk-rad/msgspec_to_pyspark_schema"
```

or from a local clone:

```bash
git clone https://github.com/sikauk-rad/msgspec_to_pyspark_schema
cd msgspec_to_pyspark_schema
pip install -e .
```

## Quick start

```python
from msgspec import Struct

from msgspec_to_pyspark_schema import (
    convert_msgspec_struct_type_to_pyspark_schema,
    convert_msgspec_structs_to_pyspark_df,
)


class Address(Struct):
    city: str
    postcode: str | None


class User(Struct):
    id: int
    name: str
    address: Address
    tags: list[str] = []


schema = convert_msgspec_struct_type_to_pyspark_schema(User)
# StructType([
#     StructField('id', LongType(), False),
#     StructField('name', StringType(), False),
#     StructField('address', StructType([
#         StructField('city', StringType(), False),
#         StructField('postcode', StringType(), True),
#     ]), False),
#     StructField('tags', ArrayType(StringType(), False), True),
# ])

users = [
    User(id = 1, name = 'Alice', address = Address(city = 'Leeds', postcode = 'LS1 1AA')),
    User(id = 2, name = 'Bob', address = Address(city = 'York', postcode = None)),
]
df = convert_msgspec_structs_to_pyspark_df(users)
df.printSchema()
```

## Converting schemas

`convert_msgspec_struct_type_to_pyspark_schema` takes a `msgspec.Struct` **class** and
returns a `pyspark.sql.types.StructType`.

```python
schema = convert_msgspec_struct_type_to_pyspark_schema(
    User,
    config = MSGSpecToPySparkSchemaConfig(integer_type = IntegerType()),
)
```

The struct is inspected with `msgspec.inspect.type_info` and every field is mapped
recursively to the equivalent Spark type. Field order is preserved.

The top-level object must be a `msgspec.Struct`. Dataclasses, `TypedDict`s and
`NamedTuple`s are supported as nested fields, but not as the top-level type.

## Creating DataFrames

`convert_msgspec_structs_to_pyspark_df` takes a sequence of struct **instances** and
returns a `pyspark.sql.DataFrame`.

```python
df = convert_msgspec_structs_to_pyspark_df(
    users,
    schema = None,                  # inferred from the type of the first struct
    config = MSGSpecToPySparkSchemaConfig(),
    verify_types = False,           # set True to check all structs share one type
    spark_session = None,           # defaults to SparkSession.builder.getOrCreate()
)
```

Behaviour:

- If `schema` is omitted, it is inferred from the type of the first struct. `config` is
  only used for this inference and is ignored when `schema` is supplied.
- If `verify_types = True`, every struct is checked to be of the same class before
  inference. A `MSGSpecToPySparkTypeError` is raised otherwise.
- If `structs` is empty, `schema` must be supplied. An empty DataFrame with that schema
  is returned. Without a schema a `MSGSpecToPySparkValueError` is raised.

Instances are converted to Python builtins with `msgspec.to_builtins` before being passed
to Spark. Types that Spark understands natively (`bytes`, `bytearray`, `datetime`,
`date`, `time`, `timedelta`, `Decimal`) are left untouched. Everything else is converted
by msgspec as usual: UUIDs become strings, enums become their values, sets and tuples
become lists, and nested structs become dictionaries.

## Configuration

Behaviour is controlled by `MSGSpecToPySparkSchemaConfig`, a frozen, keyword-only
msgspec `Struct`.

| Option                      | Default         | Description                                                                         |
|-----------------------------|-----------------|-------------------------------------------------------------------------------------|
| `integer_type`              | `LongType()`    | Spark type for `int`, integer enums, integer literals and integer struct tags. One of `ShortType`, `IntegerType` or `LongType`. |
| `float_type`                | `DoubleType()`  | Spark type for `float` and float literals. One of `FloatType` or `DoubleType`.     |
| `default_decimal_precision` | `38`            | Precision of every `DecimalType`.                                                  |
| `default_decimal_scale`     | `18`            | Scale of every `DecimalType`.                                                      |
| `any_to_string`             | `False`         | Map `Any` to a nullable `StringType` instead of raising.                           |
| `nonimplemented_to_string`  | `False`         | Map otherwise unsupported types to a nullable `StringType` instead of raising.     |

```python
from pyspark.sql.types import FloatType, IntegerType

from msgspec_to_pyspark_schema import MSGSpecToPySparkSchemaConfig

config = MSGSpecToPySparkSchemaConfig(
    integer_type = IntegerType(),
    float_type = FloatType(),
    default_decimal_precision = 18,
    default_decimal_scale = 4,
)
```

## Type mapping

| msgspec / Python type                                     | Spark type                                                |
|-----------------------------------------------------------|-----------------------------------------------------------|
| `bool`                                                    | `BooleanType`                                             |
| `int`                                                     | `config.integer_type`                                     |
| `float`                                                   | `config.float_type`                                       |
| `str`                                                     | `StringType`                                              |
| `bytes`, `bytearray`                                      | `BinaryType`                                              |
| `uuid.UUID`                                               | `StringType`                                              |
| `datetime.date`                                           | `DateType`                                                |
| `datetime.time`                                           | `TimeType`                                                |
| `datetime.timedelta`                                      | `DayTimeIntervalType`                                     |
| `datetime.datetime` with `Meta(tz = True)`                | `TimestampType`                                           |
| `datetime.datetime` (otherwise)                           | `TimestampNTZType`                                        |
| `decimal.Decimal`                                         | `DecimalType(config.default_decimal_precision, config.default_decimal_scale)` |
| `list[T]`, `set[T]`, `frozenset[T]`, `tuple[T, ...]`      | `ArrayType(T)`                                            |
| `dict[K, V]`                                              | `MapType(K, V)`                                           |
| `tuple[A, B, ...]`                                        | `StructType` with fields `_1`, `_2`, ...                  |
| `Struct`, dataclass, `TypedDict`, `NamedTuple`            | `StructType`                                              |
| `Literal[...]`                                            | Narrowest type covering all values (see below)            |
| `Enum` with `str` values                                  | `StringType`                                              |
| `Enum` with `int` values                                  | `config.integer_type`                                     |
| `T \| None`                                               | `T`, nullable                                             |
| `None`                                                    | `NullType`, nullable                                      |
| `Any`                                                     | `StringType` if `config.any_to_string`, otherwise error   |
| Anything else (e.g. `msgspec.Raw`, `msgspec.msgpack.Ext`) | `StringType` if `config.nonimplemented_to_string`, otherwise error |

`Annotated` constraints and metadata (`Meta(ge = 0)`, `Meta(description = ...)`, etc.)
are ignored for the purposes of type mapping, with the exception of `tz` on datetimes.

Literals are mapped as follows, with `None` among the values making the result nullable:

| Literal values           | Spark type             |
|--------------------------|------------------------|
| only `bool`              | `BooleanType`          |
| only `int`               | `config.integer_type`  |
| `int` and `float`        | `config.float_type`    |
| only `str`               | `StringType`           |
| only `None`              | `NullType`             |
| any other mix            | error                  |

## Nullability

A struct field is marked nullable if **either**:

- the field is not required (i.e. it has a default), or
- the field type allows `None`.

Array elements and map values are nullable if their type allows `None`. Map keys may
never be nullable, as Spark does not support this, so `dict[str | None, V]` raises an
error.

```python
class Example(Struct):
    a: int                  # LongType, not nullable
    b: int | None           # LongType, nullable
    c: int = 0              # LongType, nullable (not required)
    d: list[int | None]     # ArrayType(LongType, containsNull = True), not nullable
```

## Field names, tags and record types

**Renamed fields.** Field names are taken from msgspec's `encode_name`, so both
`rename = ...` on the struct and `msgspec.field(name = ...)` on individual fields are
respected, and match the keys produced by `msgspec.to_builtins`.

```python
class Renamed(Struct, rename = 'camel'):
    first_name: str                         # -> firstName
    last_name: str = field(name = 'surname')  # -> surname
```

**Tagged structs.** If a struct is tagged, the tag field is prepended to the schema as a
non-nullable column. String tags become `StringType`; integer tags become
`config.integer_type`.

```python
class Cat(Struct, tag = True):
    name: str

convert_msgspec_struct_type_to_pyspark_schema(Cat)
# StructType([
#     StructField('type', StringType(), False),
#     StructField('name', StringType(), False),
# ])
```

**Other record types.** Dataclasses, `TypedDict`s (including `total = False`) and
`NamedTuple`s nested within a struct are converted to `StructType`s using the same
rules.

**Recursive types.** Self-referential and mutually recursive structs cannot be
represented as a Spark schema and raise a `MSGSpecToPySparkTypeError`. Using the same
struct more than once in a schema (e.g. `home: Address` and `work: Address`) is
perfectly fine.

## Unsupported types and fallbacks

Spark has no equivalent of a general union, so only `T | None` is supported. Unions such
as `int | str` or `Cat | Dog` raise a `MSGSpecToPySparkTypeError`.

`Any` raises a `MSGSpecToPySparkTypeError` unless `config.any_to_string` is enabled.
Other unsupported types raise a `MSGSpecToPySparkNotImplementedError` unless
`config.nonimplemented_to_string` is enabled. The two options are independent:
`nonimplemented_to_string` does **not** cover `Any`.

Both fallbacks only affect the **schema**. Values are not stringified, so they must
already be strings by the time a DataFrame is built, for example by encoding them to
JSON beforehand.

## String normalisation utilities

Two helpers are included for producing SQL-friendly identifiers, such as column or
table names.

`sql_normalise_string` works on Python strings:

```python
from msgspec_to_pyspark_schema import sql_normalise_string

sql_normalise_string('Hello World!')                       # 'hello_world'
sql_normalise_string('Example_String', lowercase = False)  # 'Example_String'
sql_normalise_string('A' * 300)                            # 'aaa...' (255 characters)
```

`sql_normalise_string_col` is the PySpark equivalent and works on columns:

```python
from msgspec_to_pyspark_schema import sql_normalise_string_col

df = df.withColumn('clean', sql_normalise_string_col('raw'))
df = df.withColumn('clean', sql_normalise_string_col(F.col('raw'), lowercase = False))
```

Both apply the following steps, each of which can be disabled:

1. `sql_normalise`: replace runs of characters outside `[A-Za-z0-9_]` with a single `_`
   and strip leading and trailing underscores.
2. `lowercase`: convert to lower case.
3. `limit_to_255_chars`: truncate to 255 characters.

`sql_normalise_string_col` also accepts a custom `pattern`, either as a string or a
compiled `re.Pattern`. Spark evaluates it with Java regex semantics, and flags on a
compiled pattern are discarded, so embed any flags inline instead (e.g. `(?i)`). Nulls
remain null.

The two functions give identical results for ASCII input. For non-ASCII input, Python
and Spark lower-casing may differ in rare edge cases.

## Exceptions

All exceptions raised by the package inherit from `MSGSpecToPySparkException`, and also
from the matching built-in exception, so they can be caught either way.

| Exception                             | Built-in parent        | Raised when                                                       |
|---------------------------------------|------------------------|-------------------------------------------------------------------|
| `MSGSpecToPySparkValueError`          | `ValueError`           | An empty sequence is passed without a schema.                     |
| `MSGSpecToPySparkTypeError`           | `TypeError`            | A type cannot be represented in Spark (general unions, recursion, nullable map keys, mixed literals or enums, `Any`), the top-level type is not a `Struct`, or `verify_types` finds mixed struct types. |
| `MSGSpecToPySparkNotImplementedError` | `NotImplementedError`  | A type has no mapping and `nonimplemented_to_string` is disabled. |

```python
from msgspec_to_pyspark_schema.exceptions import MSGSpecToPySparkException
```

## Limitations

- **Decimal precision and scale** are not exposed by msgspec, so every decimal uses the
  configured defaults.
- **Time zones.** Only datetimes annotated with `Meta(tz = True)` map to the time
  zone-aware `TimestampType`. A plain `datetime` field maps to `TimestampNTZType`, so
  annotate any field that holds aware datetimes.
- **Struct constructors do not validate types.** msgspec only validates on decoding, so a
  `float` field constructed directly with an `int` value (e.g. `ratio = 1`) is passed to
  Spark as an `int`, which Spark's type verification rejects. Use float values, or build
  instances via `msgspec.convert` / decoding.
- **`omit_defaults = True`.** `msgspec.to_builtins` omits fields equal to their default,
  and Spark fills missing keys with `null`. Default values are therefore lost when
  building DataFrames from such structs.
- **Array-like structs** are emitted as lists by msgspec and matched to the schema
  positionally.

## Development

Install the package with its test dependencies:

```bash
pip install -e ".[test]"
```

Run the full test suite (requires Java for the Spark tests):

```bash
pytest
```

Run only the tests that do not need Spark:

```bash
pytest -m "not spark"
```

## Licence

Released under the MIT Licence.