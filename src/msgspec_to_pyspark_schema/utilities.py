import datetime
from decimal import Decimal
from functools import partial
import re
from uuid import UUID

from msgspec import to_builtins

from .constants import JSON_INCOMPATIBLE_CHARS


struct_to_dict = partial(
    to_builtins,
    builtin_types = [
        bytes,
        bytearray,
        UUID,
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
        If True, converts the string to lowercase using `str.casefold()`.Default is True.

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
        string = string.casefold()
    if limit_to_255_chars:
        string = string[:255]
    return string