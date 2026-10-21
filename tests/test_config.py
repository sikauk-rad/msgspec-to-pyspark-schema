from pyspark.sql import types as sqltypes
import pytest

from msgspec_to_pyspark_schema import MSGSpecToPySparkSchemaConfig
from msgspec_to_pyspark_schema.exceptions import (
    MSGSpecToPySparkException,
    MSGSpecToPySparkNotImplementedError,
    MSGSpecToPySparkTypeError,
    MSGSpecToPySparkValueError,
)


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------

def test_config_defaults() -> None:
    config = MSGSpecToPySparkSchemaConfig()

    assert config.integer_type == sqltypes.LongType()
    assert config.float_type == sqltypes.DoubleType()
    assert config.default_decimal_precision == 38
    assert config.default_decimal_scale == 18
    assert config.any_to_string is False
    assert config.nonimplemented_to_string is False


def test_config_is_frozen() -> None:
    config = MSGSpecToPySparkSchemaConfig()

    with pytest.raises(AttributeError):
        config.any_to_string = True


def test_config_is_keyword_only() -> None:
    with pytest.raises(TypeError):
        MSGSpecToPySparkSchemaConfig(sqltypes.IntegerType())


def test_config_accepts_overrides() -> None:
    config = MSGSpecToPySparkSchemaConfig(
        integer_type = sqltypes.IntegerType(),
        float_type = sqltypes.FloatType(),
        default_decimal_precision = 10,
        default_decimal_scale = 2,
        any_to_string = True,
        nonimplemented_to_string = True,
    )

    assert config.integer_type == sqltypes.IntegerType()
    assert config.float_type == sqltypes.FloatType()
    assert config.default_decimal_precision == 10
    assert config.default_decimal_scale == 2
    assert config.any_to_string is True
    assert config.nonimplemented_to_string is True


# --------------------------------------------------------------------------------------
# Exception hierarchy
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    ('exception', 'builtin_parent'),
    [
        (MSGSpecToPySparkValueError, ValueError),
        (MSGSpecToPySparkTypeError, TypeError),
        (MSGSpecToPySparkNotImplementedError, NotImplementedError),
    ],
)
def test_exceptions_subclass_package_base_and_builtin(
    exception: type[Exception],
    builtin_parent: type[Exception],
) -> None:
    assert issubclass(exception, MSGSpecToPySparkException)
    assert issubclass(exception, builtin_parent)