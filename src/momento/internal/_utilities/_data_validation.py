from datetime import timedelta
from typing import Optional, Union

from momento.errors import InvalidArgumentException

DEFAULT_STRING_CONVERSION_ERROR = "Could not decode bytes to UTF-8"


def _validate_cache_name(cache_name: str) -> None:
    if cache_name is None or not isinstance(cache_name, str):
        raise InvalidArgumentException("Cache name must be a non-empty string")


def _as_bytes(
    data: Union[str, bytes],
    error_message: Optional[str] = DEFAULT_STRING_CONVERSION_ERROR,
) -> bytes:
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, bytes):
        return data
    raise InvalidArgumentException(error_message + str(type(data)))


def _validate_ttl(ttl: timedelta) -> None:
    if not isinstance(ttl, timedelta) or ttl.total_seconds() < 0:
        raise InvalidArgumentException("TTL timedelta must be a non-negative integer")


def _validate_request_timeout(request_timeout: Optional[timedelta]) -> None:
    if request_timeout is None:
        return
    if not isinstance(request_timeout, timedelta) or request_timeout.total_seconds() <= 0:
        raise InvalidArgumentException("Request timeout must be a timedelta with a value greater than zero.")
