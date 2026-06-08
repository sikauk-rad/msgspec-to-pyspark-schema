import re

from .constants import JSON_INCOMPATIBLE_CHARS

def sql_normalise_string(
    string: str,
    sql_normalise: bool = True,
    lowercase: bool = True,
    limit_to_255_chars: bool = True,
) -> str:

    if sql_normalise:
        string = re.sub(JSON_INCOMPATIBLE_CHARS, '_', string).strip('_')
    if lowercase:
        string = string.casefold()
    if limit_to_255_chars:
        string = string[:255]
    return string