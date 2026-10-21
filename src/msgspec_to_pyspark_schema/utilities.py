import datetime
from decimal import Decimal
from functools import partial
import re
# from uuid import UUID

from msgspec import to_builtins
from pyspark.sql import Column
import pyspark.sql.functions as F

from .constants import JSON_INCOMPATIBLE_CHARS


struct_to_dict = partial(
    to_builtins,
    builtin_types = [
        bytes,
        bytearray,
        # UUID,
        datetime.datetime,
        datetime.time,
        datetime.date,
        datetime.timedelta,
        Decimal,
    ],
)

def sql_normalise_string(
    string: str,
    sql_normalise: bool = True,
    lowercase: bool = True,
    limit_to_255_chars: bool = True,
) -> str:


    """
    Normalise a string for SQL compatibility and standardisation.

    This function applies a series of optional transformations to make a string suitable 
    for use in SQL contexts, such as identifiers or column names. Transformations include
    replacing incompatible characters, converting to lowercase, and truncating the string
    to a maximum length.

    Parameters
    ----------
    string : str
        The input string to be normalised.

    sql_normalise : bool, optional
        If True, replaces characters matching `JSON_INCOMPATIBLE_CHARS` with underscores 
        ("_") and strips leading/trailing underscores. Default is True.

    lowercase : bool, optional
        If True, converts the string to lowercase using `str.lower()`.Default is True.

    limit_to_255_chars : bool, optional
        If True, truncates the string to a maximum length of 255 characters. Default is 
        True.

    Returns
    -------
    str
        The normalised string after applying the selected transformations.

    Notes
    -----
    - The character replacement pattern is defined by the global
      `JSON_INCOMPATIBLE_CHARS` regular expression.
    - Transformations are applied in the following order:
        1. SQL normalisation (character replacement and trimming)
        2. Lowercasing
        3. Length truncation

    Examples
    --------
    >>> sql_normalise_string("Hello World!", sql_normalise=True)
    'hello_world'

    >>> sql_normalise_string("Example_String", lowercase=False)
    'Example_String'

    >>> sql_normalise_string("A" * 300, limit_to_255_chars=True)
    'aaaaaaaaaa...'  # truncated to 255 characters
    """


    if sql_normalise:
        string = re.sub(
            pattern = JSON_INCOMPATIBLE_CHARS, 
            repl = '_', 
            string = string,
        ).strip('_')
    if lowercase:
        string = string.lower()
    if limit_to_255_chars:
        string = string[:255]
    return string


def sql_normalise_string_col(
    col: str | Column,
    sql_normalise: bool = True,
    lowercase: bool = True,
    limit_to_255_chars: bool = True,
    pattern: str | re.Pattern = JSON_INCOMPATIBLE_CHARS,
) -> Column:

    """
    Normalise a string column for SQL compatibility and standardisation.

    PySpark equivalent of `sql_normalise_string`. Transformations are applied in order:
        1. SQL normalisation (replace incompatible chars with "_" and strip leading/
        trailing "_")
        2. Lowercasing
        3. Truncation to 255 characters

    Parameters
    ----------
    col : str or Column
        Column name or Column expression containing the strings to normalise.
    sql_normalise : bool, optional
        If True, replaces characters matching `pattern` with "_" and strips leading / 
        trailing underscores. Default is True.
    lowercase : bool, optional
        If True, converts the string to lowercase. Default is True.
    limit_to_255_chars : bool, optional
        If True, truncates the string to a maximum of 255 characters. Default is True.
    pattern : str or re.Pattern, optional
        Regex for incompatible characters. Defaults to `JSON_INCOMPATIBLE_CHARS`.

    Returns
    -------
    Column
        A Column expression with the transformations applied. Nulls stay null.

    Notes
    -----
    - Spark evaluates `pattern` with Java regex semantics. Flags on a compiled Python 
      `re.Pattern` are discarded; embed them inline instead (e.g., ``(?i)``).

    Examples
    --------
    >>> df.withColumn("clean", sql_normalise_string_col("raw"))
    >>> df.withColumn("clean", sql_normalise_string_col(F.col("raw"), lowercase=False))
    """

    c = F.col(col) if isinstance(col, str) else col

    # Accept either a raw string or a compiled Python regex
    pattern_str = pattern.pattern if isinstance(pattern, re.Pattern) else pattern

    if sql_normalise:
        # Equivalent of re.sub(pattern, "_", s)
        c = F.regexp_replace(c, pattern_str, "_")
        # Equivalent of .strip("_")
        c = F.regexp_replace(c, r"^_+|_+$", "")

    if lowercase:
        # Approximation of str.casefold()
        c = F.lower(c)

    if limit_to_255_chars:
        # Equivalent of s[:255]. Spark's substring is 1-indexed.
        c = F.substring(c, 1, 255)

    return c