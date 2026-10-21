from itertools import product
import re

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pytest

from msgspec_to_pyspark_schema import sql_normalise_string, sql_normalise_string_col


# --------------------------------------------------------------------------------------
# sql_normalise_string
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    ('string', 'expected'),
    [
        ('Hello World!', 'hello_world'),
        ('__leading_and_trailing__', 'leading_and_trailing'),
        ('multiple---separators   here', 'multiple_separators_here'),
        ('MiXeD_CaSe_123', 'mixed_case_123'),
        ('Café', 'caf'),
        ('!!!', ''),
        ('', ''),
    ],
)
def test_sql_normalise_string_defaults(string: str, expected: str) -> None:
    assert sql_normalise_string(string) == expected


def test_sql_normalise_string_preserves_case_when_requested() -> None:
    assert sql_normalise_string('Example_String', lowercase = False) == 'Example_String'


def test_sql_normalise_string_without_normalisation() -> None:
    assert sql_normalise_string('Hello World!', sql_normalise = False) == 'hello world!'


def test_sql_normalise_string_truncates_to_255() -> None:
    assert sql_normalise_string('A' * 300) == 'a' * 255


def test_sql_normalise_string_truncates_after_normalisation() -> None:
    assert sql_normalise_string('#' * 10 + 'b' * 300) == 'b' * 255


def test_sql_normalise_string_without_truncation() -> None:
    assert sql_normalise_string('a' * 300, limit_to_255_chars = False) == 'a' * 300


def test_sql_normalise_string_all_disabled_is_identity() -> None:
    string = '  Some Raw String!  '
    assert sql_normalise_string(
        string,
        sql_normalise = False,
        lowercase = False,
        limit_to_255_chars = False,
    ) == string


# --------------------------------------------------------------------------------------
# sql_normalise_string_col
# --------------------------------------------------------------------------------------

PARITY_STRINGS = [
    'Hello World!',
    '__leading_and_trailing__',
    'multiple---separators   here',
    'MiXeD_CaSe_123',
    '!!!',
    '',
    'a' * 300,
    '#' * 10 + 'B' * 300,
]


@pytest.mark.parametrize(
    ('sql_normalise', 'lowercase', 'limit_to_255_chars'),
    list(product([True, False], repeat = 3)),
)
def test_column_matches_python_implementation(
    spark: SparkSession,
    sql_normalise: bool,
    lowercase: bool,
    limit_to_255_chars: bool,
) -> None:
    flags = dict(
        sql_normalise = sql_normalise,
        lowercase = lowercase,
        limit_to_255_chars = limit_to_255_chars,
    )
    df = spark.createDataFrame(
        data = [(string,) for string in PARITY_STRINGS],
        schema = 'raw string',
    )
    rows = df.select(
        'raw',
        sql_normalise_string_col('raw', **flags).alias('clean'),
    ).collect()

    for row in rows:
        assert row.clean == sql_normalise_string(row.raw, **flags)


def test_column_keeps_nulls(spark: SparkSession) -> None:
    df = spark.createDataFrame(data = [(None,)], schema = 'raw string')
    assert df.select(sql_normalise_string_col('raw')).collect()[0][0] is None


def test_column_accepts_column_expression(spark: SparkSession) -> None:
    df = spark.createDataFrame(data = [('Hello World!',)], schema = 'raw string')
    result = df.select(
        sql_normalise_string_col(F.col('raw'), lowercase = False)
    ).collect()[0][0]
    assert result == 'Hello_World'


@pytest.mark.parametrize(
    'pattern',
    [r'[^a-z]+', re.compile(r'[^a-z]+')],
)
def test_column_accepts_custom_pattern(
    spark: SparkSession,
    pattern: str | re.Pattern,
) -> None:
    df = spark.createDataFrame(data = [('abc123DEF',)], schema = 'raw string')
    result = df.select(
        sql_normalise_string_col('raw', lowercase = False, pattern = pattern)
    ).collect()[0][0]
    assert result == 'abc'