from datetime import timedelta
from typing import List, Optional, Union

from momento.errors import InvalidArgumentException
from momento.typing import TListValues

DEFAULT_STRING_CONVERSION_ERROR = "Could not decode bytes to UTF-8"
DEFAULT_LIST_CONVERSION_ERROR = "Could not decode List[bytes] to UTF-8"


def _is_valid_name(name: str) -> bool:
    return name is not None and isinstance(name, str)


def _validate_cache_name(cache_name: str) -> None:
    if not _is_valid_name(cache_name):
        raise InvalidArgumentException("Cache name must be a non-empty string")


def _validate_list_name(list_name: str) -> None:
    if not _is_valid_name(list_name):
        raise InvalidArgumentException("List name must be a non-empty string")


def _as_bytes(
    data: Union[str, bytes],
    error_message: Optional[str] = DEFAULT_STRING_CONVERSION_ERROR,
) -> bytes:
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, bytes):
        return data
    raise InvalidArgumentException(error_message + str(type(data)))


def _list_as_bytes(values: TListValues, error_message: Optional[str] = DEFAULT_LIST_CONVERSION_ERROR) -> List[bytes]:
    return [_as_bytes(value) for value in values]


def _validate_ttl(ttl: timedelta) -> None:
    if not isinstance(ttl, timedelta) or ttl.total_seconds() < 0:
        raise InvalidArgumentException("TTL timedelta must be a non-negative integer")


def _validate_request_timeout(request_timeout: Optional[timedelta]) -> None:
    if request_timeout is None:
        return
    if not isinstance(request_timeout, timedelta) or request_timeout.total_seconds() <= 0:
        raise InvalidArgumentException("Request timeout must be a timedelta with a value greater than zero.")
