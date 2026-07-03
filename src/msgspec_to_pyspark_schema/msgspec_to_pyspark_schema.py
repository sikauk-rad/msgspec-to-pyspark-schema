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
        If the inspected object is not a ``msgspec.inspect.StructType``.
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
    >>> convert_msgspec_struct_to_pyspark_schema(Address)
    StructType([...])
    """


    type_info = msginspect.type_info(struct)

    if not isinstance(type_info, msginspect.StructType):
        raise MSGSpecToPySparkTypeError(
            f"Expected a msgspec StructType from inspection, got {type(type_info)!r}"
        )

    # Track recursion to avoid infinite loops on self-referential types.
    ids_seen: set[int] = set()
    return make_spark_struct_from_msgspec_struct_type(
        struct_type = type_info, 
        ids_seen = ids_seen,
        config = config,
    )


def make_spark_struct_from_msgspec_struct_type(
    struct_type: msginspect.StructType,
    ids_seen: set[int],
    *,
    config: MSGSpecToPySparkSchemaConfig = MSGSpecToPySparkSchemaConfig(),
) -> sqltypes.StructType:


    """
    Build a PySpark ``StructType`` from a msgspec ``StructType``.

    This is the recursive worker used to convert nested msgspec struct definitions into 
    Spark schemas. It tracks previously visited type objects to detect recursion and 
    prevent infinite loops when processing nested or self-referential types.

    Parameters
    ----------
    struct_type : msgspec.inspect.StructType
        The inspected msgspec struct type to convert.

    ids_seen : set of int
        A set of object ids for struct types currently being processed. This is used to 
        detect recursive references during traversal.

    config : MSGSpecToPySparkSchemaConfig, optional
        Configuration controlling how individual field types are mapped to Spark SQL 
        types.

    Returns
    -------
    pyspark.sql.types.StructType
        A Spark ``StructType`` composed of fields converted from the msgspec struct 
        definition.

    Raises
    ------
    MSGSpecToPySparkTypeError
        If a recursive or self-referential type is encountered.

    Notes
    -----
    Each field is converted using ``make_spark_datatype_from_msgspec_type``.
    A field is marked nullable if either:

    - the field is not required
    - the field type itself allows ``None``

    Examples
    --------
    >>> type_info = msginspect.type_info(MyStruct)
    >>> make_spark_struct_from_msgspec_struct_type(type_info, ids_seen=set())
    StructType([...])
    """


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
        spark_type, type_allows_none = make_spark_datatype_from_msgspec_type(
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


def make_spark_datatype_from_msgspec_type(
    msgspec_type: msginspect.Type,
    ids_seen: set[int],
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

    ids_seen : set of int
        A set of object ids used to detect recursive struct definitions while converting 
        nested types.

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
        If a type cannot be represented in Spark, such as a general union type, an 
        untyped tuple, or a map with nullable keys.
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
    - enums are represented as strings
    - literals are represented by the narrowest practical Spark type

    If ``config.any_to_string`` is enabled, ``Any`` is mapped to ``StringType``. 
    If ``config.nonimplemented_to_string`` is enabled, unsupported types are also 
    mapped to ``StringType``.

    Examples
    --------
    >>> t = msginspect.type_info(int | None)
    >>> make_spark_datatype_from_msgspec_type(t, ids_seen=set())
    (IntegerType(), True)

    >>> t = msginspect.type_info(list[str])
    >>> make_spark_datatype_from_msgspec_type(t, ids_seen=set())
    (ArrayType(StringType(), containsNull=False), False)
    """

    make_kwargs = dict(
        ids_seen = ids_seen,
        config = config,
    )

    make_spark_datatype = partial(
        make_spark_datatype_from_msgspec_type,
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
        # if not item_types:
            # raise MSGSpecToPySparkTypeError(f'Untyped tuple field {msgspec_type!r}.') 
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
        return make_spark_struct_from_msgspec_struct_type(
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


    """
    Convert a sequence of msgspec struct instances into a PySpark DataFrame.

    If no schema is provided, the schema is inferred from the type of the supplied struct
    instances. When required, the caller may request validation that all instances in the
    sequence share the same concrete struct type.

    Parameters
    ----------
    structs : Sequence[T]
        A sequence of msgspec ``Struct`` instances to convert.

    schema : PySparkSchemaType or None, optional
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
        schema = convert_msgspec_struct_type_to_pyspark_schema(
            struct = struct_type,
            config = config,
        )

    return spark.createDataFrame(
        data = struct_to_dict(structs),
        schema = schema,
    )