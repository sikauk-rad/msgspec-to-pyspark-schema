from __future__ import annotations
from collections.abc import Sequence
from functools import partial
import operator as op

from msgspec import Struct
import msgspec.inspect as msginspect
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as sqltypes

from .constants import MSGSPEC_TYPE_TO_SPARK_MAP
from .datatypes import PySparkSchemaType, MSGSpecToPySparkSchemaConfig
from .exceptions import (
    MSGSpecToPySparkValueError, 
    MSGSpecToPySparkTypeError, 
    MSGSpecToPySparkNotImplementedError,
)
from .utilities import struct_to_dict


def convert_msgspec_struct_to_pyspark_schema(
    struct: type[Struct],
    *,
    config: MSGSpecToPySparkSchemaConfig = MSGSpecToPySparkSchemaConfig(),
) -> sqltypes.StructType:

    """
    Convert a msgspec.Struct class into a pyspark.sql.types.StructType schema.

    - Uses msgspec.inspect.type_info (msgspec's native type inspection API).
    - Uses Field.encode_name as the Spark column name (respects msgspec.field(name=...)).
    - Sets nullable=True when a field is not required OR its type allows None.
    - Recurses through nested Structs, lists/sets, dicts, tuples.
    - Raises TypeError for unions that are not Optional[T] (Spark has no union type).
    """

    type_info = msginspect.type_info(struct)

    if not isinstance(type_info, msginspect.StructType):
        raise MSGSpecToPySparkTypeError(
            f"Expected a msgspec StructType from inspection, got {type(type_info)!r}"
        )

    # Track recursion to avoid infinite loops on self-referential types.
    ids_seen: set[int] = set()
    return make_spark_structtype_from_msgspec(
        struct_type = type_info, 
        ids_seen = ids_seen,
        config = config,
    )


def make_spark_structtype_from_msgspec(
    struct_type: msginspect.StructType,
    ids_seen: set[int],
    *,
    config: MSGSpecToPySparkSchemaConfig = MSGSpecToPySparkSchemaConfig(),
) -> sqltypes.StructType:

    if (struct_type_id := id(struct_type)) in ids_seen:
        raise MSGSpecToPySparkTypeError(
            "Recursive (self-referential) types are not representable as Spark schemas."
        )
    else:
        ids_seen.add(struct_type_id)

    spark_fields: list[sqltypes.StructField] = []

    for struct_field in struct_type.fields:
        name, msgspec_type, required = op.attrgetter(
            'encode_name',
            'type',
            'required',
        )(struct_field)
        spark_type, type_allows_none = make_spark_datatype_from_msgspec(
            msgspec_type = msgspec_type, 
            ids_seen = ids_seen,
            config = config,
        )

        # nullable if either:
        # - not required (defaults mean the field may be omitted in input and become null
        # in Spark rows)
        # - type allows None
        spark_fields.append(
            sqltypes.StructField(
                name = name, 
                dataType = spark_type, 
                nullable = (not required) or type_allows_none,
            ),
        )

    ids_seen.remove(struct_type_id)
    return sqltypes.StructType(spark_fields)


def make_spark_datatype_from_msgspec(
    msgspec_type: msginspect.Type,
    ids_seen: set[int],
    config: MSGSpecToPySparkSchemaConfig = MSGSpecToPySparkSchemaConfig(),
) -> tuple[sqltypes.DataType, bool]:

    """
    Returns (spark_datatype, allows_none).
    `allows_none` indicates whether the value itself may be None.
    """

    make_kwargs = dict(
        ids_seen = ids_seen,
        config = config,
    )

    make_spark_datatype = partial(
        make_spark_datatype_from_msgspec,
        **make_kwargs,
    )

    # Handle Optional / Union first
    if isinstance(msgspec_type, msginspect.UnionType):
        subtypes = (
            subtype for subtype in msgspec_type.types
            if not isinstance(subtype, msginspect.NoneType)
        )
        try:
            base_datatype = next(subtypes)
        except StopIteration: # no subtypes present, element is always null
            return sqltypes.NullType(), True

        try:
            next(subtypes)
        except StopIteration: # one non-none subtype present, valid case
            spark_datatype, _ = make_spark_datatype(
                msgspec_type = base_datatype,
            )
            return spark_datatype, True # union included None => nullable
        else:
            raise MSGSpecToPySparkTypeError(
                "Unsupported union type for Spark schema (only T|None is supported):"
                f" {msgspec_type!r}"
            )


    elif isinstance(msgspec_type, msginspect.IntType):
        return config.integer_type, False


    elif isinstance(msgspec_type, msginspect.FloatType):
        return config.float_type, False


    elif (spark_datatype := MSGSPEC_TYPE_TO_SPARK_MAP.get(type(msgspec_type), None)):
        return spark_datatype, False


    elif isinstance(msgspec_type, msginspect.DecimalType):
        precision = getattr(msgspec_type, "precision", config.default_decimal_precision)
        scale = getattr(msgspec_type, "scale", config.default_decimal_scale)
        return sqltypes.DecimalType(
            precision = precision, 
            scale = scale,
        ), False


    elif isinstance(msgspec_type, msginspect.CollectionType):
        element_datatype = msgspec_type.item_type
        element_spark_datatype, element_nullability = make_spark_datatype(
            msgspec_type = element_datatype, 
        )
        return sqltypes.ArrayType(
            elementType = element_spark_datatype,
            containsNull = element_nullability,
        ), False


    elif isinstance(msgspec_type, msginspect.DictType):
        key_datatype, value_datatype = op.attrgetter(
            'key_type',
            'value_type',
        )(msgspec_type)
        key_spark_datatype, key_nullability = make_spark_datatype(
            msgspec_type = key_datatype, 
        )
        if key_nullability or isinstance(key_spark_datatype, sqltypes.NullType):
            raise MSGSpecToPySparkTypeError("Spark MapType does not support nullable/None keys.")
        value_spark_datatype, value_nullability = make_spark_datatype(
            msgspec_type = value_datatype, 
        )
        return sqltypes.MapType(
            keyType = key_spark_datatype, 
            valueType = value_spark_datatype, 
            valueContainsNull = value_nullability,
        ), False


    elif isinstance(msgspec_type, msginspect.TupleType):
        item_types = getattr(msgspec_type, 'item_types', None)
        if not item_types:
            raise MSGSpecToPySparkTypeError(f'Untyped tuple field {msgspec_type!r}.') 
        spark_fields: list[sqltypes.StructField] = []
        for n, element_type in enumerate(item_types, start=1):
            element_spark_datatype, element_nullability = make_spark_datatype(
                msgspec_type = element_type, 
            )
            spark_fields.append(
                sqltypes.StructField(
                    name = f"_{n}", 
                    dataType = element_spark_datatype, 
                    nullable = element_nullability,
                )
            )
        return sqltypes.StructType(spark_fields), False


    elif isinstance(msgspec_type, msginspect.StructType):
        return make_spark_structtype_from_msgspec(
            struct_type = msgspec_type, 
            **make_kwargs,
        ), False


    elif isinstance(msgspec_type, msginspect.LiteralType):
        nullable, value_types = False, set()
        for value in msgspec_type.values:
            if value is None:
                nullable = True
            else:
                value_types.add(type(value))

        if not value_types:
            return sqltypes.NullType(), True
        elif value_types <= {bool}:
            return sqltypes.BooleanType(), nullable
        elif value_types <= {int}:
            return sqltypes.IntegerType(), nullable
        else:
            max_len = max(map(len, map(str, msgspec_type.values)))
            return sqltypes.VarcharType(max_len), nullable


    elif isinstance(msgspec_type, msginspect.EnumType):
        # Represent enums as strings (name/value representation is application-specific)
        return sqltypes.StringType(), False


    elif isinstance(msgspec_type, msginspect.AnyType):
        if config.any_to_string:
            return sqltypes.StringType(), True
        else:
            raise MSGSpecToPySparkTypeError('`Any` type not supported.')

    elif config.nonimplemented_to_string:
        return sqltypes.StringType(), True
    
    else:
        raise MSGSpecToPySparkNotImplementedError(f'Unsupported type: {msgspec_type!r}.')


def convert_msgspec_structs_to_pyspark_df[T: Struct](
    structs: Sequence[T],
    schema: PySparkSchemaType | None = None,
    *,
    config: MSGSpecToPySparkSchemaConfig = MSGSpecToPySparkSchemaConfig(),
    verify_types: bool = False,
    spark_session: SparkSession | None = None,
) -> DataFrame:

    spark = spark_session or SparkSession.builder.getOrCreate()

    if not structs:
        if not schema:
            raise MSGSpecToPySparkValueError(
                'schema must be defined if no data is provided.'
            )
        else:
            return spark.createDataFrame(
                data = [],
                schema = schema,
            )

    if not schema:
        all_types = map(type, structs)
        if verify_types:
            all_types = {*all_types}
            if len(all_types) > 1:
                raise MSGSpecToPySparkTypeError(
                    f'struct types within structs are not consistent: {all_types}.'
                )
        struct_type = next(iter(all_types))
        schema = convert_msgspec_struct_to_pyspark_schema(
            struct = struct_type,
            config = config,
        )

    return spark.createDataFrame(
        data = struct_to_dict(structs),
        schema = schema,
    )