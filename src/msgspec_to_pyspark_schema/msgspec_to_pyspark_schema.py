from __future__ import annotations
from collections.abc import Sequence
from functools import partial
import operator as op

from msgspec import Struct
import msgspec.inspect as msginspect
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as sqltypes

from .constants import MSGSPEC_TYPE_TO_SPARK_MAP, RECORD_TYPES
from .datatypes import MSGSpecToPySparkSchemaConfig, MakeKwargs
from .exceptions import (
    MSGSpecToPySparkValueError, 
    MSGSpecToPySparkTypeError, 
    MSGSpecToPySparkNotImplementedError,
)
from .utilities import struct_to_dict


def convert_msgspec_struct_type_to_pyspark_schema(
    struct: type[Struct],
    *,
    config: MSGSpecToPySparkSchemaConfig = MSGSpecToPySparkSchemaConfig(),
) -> sqltypes.StructType:


    """
    Convert a ``msgspec.Struct`` class into a PySpark ``StructType`` schema.

    The input struct is inspected using ``msgspec.inspect.type_info`` and each field is 
    recursively mapped to an equivalent PySpark SQL type. Field names are taken from 
    ``encode_name``, so any aliases defined with ``msgspec.field(name=...)`` are 
    preserved.

    Parameters
    ----------
    struct : type[msgspec.Struct]
        The ``msgspec.Struct`` class to convert.

    config : MSGSpecToPySparkSchemaConfig, optional
        Configuration controlling type mapping behaviour, such as the Spark integer and
        float types to use, default decimal precision and scale, and fallback handling 
        for unsupported types.

    Returns
    -------
    pyspark.sql.types.StructType
        A PySpark schema representing the given msgspec struct.

    Raises
    ------
    MSGSpecToPySparkTypeError
        If the inspected object is not a ``msgspec.inspect.StructType``, or if the 
        struct contains a type Spark cannot represent (e.g., a recursive type, a union 
        other than ``T | None``, a map with nullable keys, or ``Any`` when 
        ``config.any_to_string`` is disabled).
    MSGSpecToPySparkNotImplementedError
        If the struct contains a type that cannot be represented and no configured 
        fallback applies.

    Notes
    -----
    Fields are marked nullable when either of the following is true:

    - the field is not required
    - the field type allows ``None``

    Recursive or self-referential structures are not supported because Spark schemas 
    cannot represent them safely.

    Examples
    --------
    >>> class Address(Struct):
    ...     city: str
    ...     postcode: str | None
    ...
    >>> convert_msgspec_struct_type_to_pyspark_schema(Address)
    StructType([...])
    """


    type_info = msginspect.type_info(struct)

    if not isinstance(type_info, msginspect.StructType):
        raise MSGSpecToPySparkTypeError(
            f"Expected a msgspec StructType from inspection, got {type(type_info)}"
        )

    # Track recursion to avoid infinite loops on self-referential types.
    seen: set[type] = set()
    return make_spark_struct_from_msgspec_struct_type(
        struct_type = type_info, 
        seen = seen,
        config = config,
    )


def make_spark_struct_from_msgspec_struct_type(
    struct_type: (
        msginspect.StructType | msginspect.DataclassType 
        |  
        msginspect.TypedDictType | msginspect.NamedTupleType
    ), 
    seen: set[type], 
    *, 
    config: MSGSpecToPySparkSchemaConfig,
) -> sqltypes.StructType:

    cls = struct_type.cls
    if cls in seen:
        raise MSGSpecToPySparkTypeError(
            f"Recursive type {cls.__qualname__} is not representable as a Spark schema."
        )
    else:
        seen.add(cls)

    try:
        spark_fields: list[sqltypes.StructField] = []

        # Tagged structs: to_builtins emits the tag (first element if array_like).
        # Only msgspec Structs carry `tag_field`; dataclasses, TypedDicts and 
        # NamedTuples do not, hence the default.
        if (
            (tag_field := getattr(struct_type, 'tag_field', None)) is not None
            and
            (tag := getattr(struct_type, 'tag', None)) is not None
        ):
            spark_fields.append(
                sqltypes.StructField(
                    name = tag_field, 
                    dataType = (
                        sqltypes.StringType() 
                        if isinstance(tag, str)
                        else config.integer_type
                    ), 
                    nullable = False,
                )
            )

        for field in struct_type.fields:
            spark_type, allows_none = make_spark_datatype_from_msgspec_type(
                msgspec_type = field.type, 
                seen = seen, 
                config = config,
            )
            spark_fields.append(
                sqltypes.StructField(
                    name = field.encode_name, 
                    dataType = spark_type, 
                    nullable = (not field.required) or allows_none,
                )
            )

        return sqltypes.StructType(spark_fields)

    finally:
        seen.discard(cls)


def make_spark_datatype_from_msgspec_type(
    msgspec_type: msginspect.Type,
    seen: set[type],
    config: MSGSpecToPySparkSchemaConfig = MSGSpecToPySparkSchemaConfig(),
) -> tuple[sqltypes.DataType, bool]:


    """
    Convert a msgspec-inspected type into a PySpark SQL data type.

    This function maps a msgspec type descriptor to the corresponding Spark SQL type and 
    also reports whether values of that type may be ``None``. It supports primitive 
    types, optionals, collections, mappings, tuples, nested structs, literals, enums, 
    decimals, and ``Any`` according to the supplied configuration.

    Parameters
    ----------
    msgspec_type : msgspec.inspect.Type
        The msgspec type descriptor to convert.

    seen : set of type
        A set of record classes currently being converted, used to detect recursive 
        definitions while converting nested types.

    config : MSGSpecToPySparkSchemaConfig, optional
        Configuration for type mapping and fallback behaviour. This may define default 
        numeric types, decimal precision and scale, and whether unsupported or ``Any`` 
        types should fall back to strings.

    Returns
    -------
    tuple[pyspark.sql.types.DataType, bool]
        A pair ``(spark_datatype, allows_none)`` where:

        - ``spark_datatype`` is the mapped Spark SQL type
        - ``allows_none`` indicates whether the value itself may be ``None``

    Raises
    ------
    MSGSpecToPySparkTypeError
        If a type cannot be represented in Spark, such as a general union type, a 
        recursive type, a mixed-type literal or enum, or a map with nullable keys.
    MSGSpecToPySparkNotImplementedError
        If the type is unsupported and no fallback option is enabled.

    Notes
    -----
    The following conversion rules are applied:

    - ``T | None`` is treated as nullable ``T``
    - unions with more than one non-``None`` subtype are rejected
    - collections are converted to ``ArrayType``
    - mappings are converted to ``MapType`` with non-nullable keys only
    - tuples are converted to ``StructType`` with positional field names (e.g., ``_1``, 
    ``_2``)
    - enums are represented by their values, as ``config.integer_type`` for int enums 
    and ``StringType`` for str enums
    - literals are represented by the narrowest practical Spark type
    - decimals always use ``config.default_decimal_precision`` and 
    ``config.default_decimal_scale``, as msgspec does not expose either

    If ``config.any_to_string`` is enabled, ``Any`` is mapped to ``StringType``. 
    If ``config.nonimplemented_to_string`` is enabled, unsupported types are also 
    mapped to ``StringType``. In both cases only the schema is affected; values are not
    stringified and must already be strings when building a DataFrame.

    Examples
    --------
    >>> t = msginspect.type_info(int | None)
    >>> make_spark_datatype_from_msgspec_type(t, seen=set())
    (LongType(), True)

    >>> t = msginspect.type_info(list[str])
    >>> make_spark_datatype_from_msgspec_type(t, seen=set())
    (ArrayType(StringType(), False), False)
    """

    make_kwargs = MakeKwargs(
        seen = seen,
        config = config,
    )

    make_spark_datatype = partial(
        make_spark_datatype_from_msgspec_type,
        **make_kwargs,
    )

    if isinstance(msgspec_type, msginspect.Metadata):
        return make_spark_datatype(
            msgspec_type = msgspec_type.type, 
        )

    # Handle Optional / Union first
    elif isinstance(msgspec_type, msginspect.UnionType):
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
                f" {msgspec_type}"
            )


    elif isinstance(msgspec_type, msginspect.IntType):
        return config.integer_type, False


    elif isinstance(msgspec_type, msginspect.FloatType):
        return config.float_type, False


    elif isinstance(msgspec_type, msginspect.NoneType):
        return sqltypes.NullType(), True


    elif isinstance(msgspec_type, msginspect.DateTimeType):
        return (
            sqltypes.TimestampType()
            if msgspec_type.tz
            else sqltypes.TimestampNTZType() 
        ), False


    elif (
        spark_datatype 
        := 
        MSGSPEC_TYPE_TO_SPARK_MAP.get(type(msgspec_type), None)
    ) is not None:
        return spark_datatype, False


    elif isinstance(msgspec_type, msginspect.DecimalType):
        # msgspec does not expose precision / scale, so always use the configured values.
        return sqltypes.DecimalType(
            precision = config.default_decimal_precision, 
            scale = config.default_decimal_scale,
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
            raise MSGSpecToPySparkTypeError(
                "Spark MapType does not support nullable/None keys."
            )
        value_spark_datatype, value_nullability = make_spark_datatype(
            msgspec_type = value_datatype, 
        )
        return sqltypes.MapType(
            keyType = key_spark_datatype, 
            valueType = value_spark_datatype, 
            valueContainsNull = value_nullability,
        ), False


    elif isinstance(msgspec_type, msginspect.TupleType):
        spark_fields: list[sqltypes.StructField] = []
        for n, element_type in enumerate(msgspec_type.item_types, start = 1):
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


    elif isinstance(msgspec_type, RECORD_TYPES):
        return make_spark_struct_from_msgspec_struct_type(
            struct_type = msgspec_type, 
            seen = seen, 
            config = config,
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
            return config.integer_type, nullable
        elif value_types <= {float, int}:
            return config.float_type, nullable
        elif value_types <= {str}:
            return sqltypes.StringType(), nullable
        else:
            raise MSGSpecToPySparkTypeError(
                f"Mixed-type Literal not supported: {msgspec_type.values}."
            )


    elif isinstance(msgspec_type, msginspect.EnumType):
        # msgspec encodes enums by value, so map on the type of the member values.
        value_types = {type(m.value) for m in msgspec_type.cls}
        if value_types <= {int}:
            return config.integer_type, False
        elif value_types <= {str}:
            return sqltypes.StringType(), False
        else:
            raise MSGSpecToPySparkTypeError(
                f"Mixed-type enum not supported: {msgspec_type.cls}."
            )


    elif isinstance(msgspec_type, msginspect.AnyType):
        if config.any_to_string:
            return sqltypes.StringType(), True
        else:
            raise MSGSpecToPySparkTypeError('`Any` type not supported.')

    elif config.nonimplemented_to_string:
        return sqltypes.StringType(), True
    
    else:
        raise MSGSpecToPySparkNotImplementedError(f'Unsupported type: {msgspec_type}.')


def convert_msgspec_structs_to_pyspark_df[T: Struct](
    structs: Sequence[T],
    schema: sqltypes.StructType | None = None,
    *,
    config: MSGSpecToPySparkSchemaConfig = MSGSpecToPySparkSchemaConfig(),
    verify_types: bool = False,
    spark_session: SparkSession | None = None,
) -> DataFrame:

    """
    Convert a sequence of msgspec struct instances into a PySpark DataFrame.

    If no schema is provided, the schema is inferred from the type of the supplied struct
    instances. When required, the caller may request validation that all instances in the
    sequence share the same concrete struct type.

    Parameters
    ----------
    structs : Sequence[T]
        A sequence of msgspec ``Struct`` instances to convert.

    schema : pyspark.sql.types.StructType or None, optional
        The Spark schema to apply when constructing the DataFrame. If omitted, the schema
        is inferred from the struct type. This parameter must be provided when 
        ``structs`` is empty.

    config : MSGSpecToPySparkSchemaConfig, optional
        Configuration used during schema inference. Ignored if ``schema`` is supplied 
        explicitly.

    verify_types : bool, optional
        If ``True``, validate that all items in ``structs`` are instances of the same 
        struct class before inferring the schema. If ``False``, the schema is inferred 
        from the first available type.

    spark_session : pyspark.sql.SparkSession or None, optional
        The Spark session to use. If omitted, the active session is retrieved or created
        via ``SparkSession.builder.getOrCreate()``.

    Returns
    -------
    pyspark.sql.DataFrame
        A Spark DataFrame containing the struct data.

    Raises
    ------
    MSGSpecToPySparkValueError
        If ``structs`` is empty and no schema is provided.
    MSGSpecToPySparkTypeError
        If ``verify_types`` is enabled and multiple struct types are found.

    Notes
    -----
    Struct instances are converted to dictionaries using ``utilities.struct_to_dict``
    before DataFrame creation.

    When ``structs`` is empty:

    - a provided ``schema`` is used to create an empty DataFrame
    - absence of ``schema`` raises an error because type inference is not
      possible

    Examples
    --------
    >>> rows = [User(id=1, name="Alice"), User(id=2, name="Bob")]
    >>> df = convert_msgspec_structs_to_pyspark_df(rows)
    >>> df.printSchema()

    >>> empty_df = convert_msgspec_structs_to_pyspark_df([], schema=my_schema)
    """

    if not structs and schema is None:
        raise MSGSpecToPySparkValueError(
            'schema must be defined if no data is provided.'
        )

    spark = spark_session or SparkSession.builder.getOrCreate()

    if not structs and schema:
        return spark.createDataFrame(
            data = [],
            schema = schema,
        )

    elif schema is None:
        all_types = map(type, structs)
        if verify_types:
            all_types = {*all_types}
            if len(all_types) > 1:
                raise MSGSpecToPySparkTypeError(
                    f'struct types within structs are not consistent: {all_types}.'
                )
        struct_type = next(iter(all_types))
        schema = convert_msgspec_struct_type_to_pyspark_schema(
            struct = struct_type,
            config = config,
        )

    return spark.createDataFrame(
        data = struct_to_dict(structs),
        schema = schema,
    )