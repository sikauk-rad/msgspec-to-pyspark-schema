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