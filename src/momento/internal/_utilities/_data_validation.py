from datetime import timedelta
from typing import Optional, Union

from momento.errors import InvalidArgumentException
from momento.typing import TListValues, TListValuesBytes

DEFAULT_STRING_CONVERSION_ERROR = "Could not decode bytes to UTF-8"
DEFAULT_LIST_CONVERSION_ERROR = "Could not decode List[bytes] to UTF-8"


def _validate_name(name: str, field_name: str) -> None:
    if not isinstance(name, str):
        raise InvalidArgumentException(f"{field_name} must be a string")
    if name == "":
        raise InvalidArgumentException(f"{field_name} must not be empty")


def _validate_cache_name(cache_name: str) -> None:
    _validate_name(cache_name, "Cache name")


def _validate_list_name(list_name: str) -> None:
    _validate_name(list_name, "List name")


def _as_bytes(
    data: Union[str, bytes],
    error_message: Optional[str] = DEFAULT_STRING_CONVERSION_ERROR,
) -> bytes:
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, bytes):
        return data
    raise InvalidArgumentException(f"{error_message}{type(data)}")


def _list_as_bytes(
    values: TListValues, error_message: Optional[str] = DEFAULT_LIST_CONVERSION_ERROR
) -> TListValuesBytes:
    if not isinstance(values, list):
        raise InvalidArgumentException(f"{error_message}{type(values)}")
    return [_as_bytes(value) for value in values]


def _validate_timedelta_ttl(ttl: Optional[timedelta], field_name: str) -> None:
    if not isinstance(ttl, timedelta):
        raise InvalidArgumentException(f"{field_name} must be a timedelta.")
    if ttl.total_seconds() <= 0:
        raise InvalidArgumentException(f"{field_name} must be a positive amount of time.")


def _validate_ttl(ttl: Optional[timedelta]) -> None:
    _validate_timedelta_ttl(ttl=ttl, field_name="TTL")


def _validate_request_timeout(request_timeout: Optional[timedelta]) -> None:
    if request_timeout is None:
        return
    _validate_timedelta_ttl(ttl=request_timeout, field_name="Request timeout")
