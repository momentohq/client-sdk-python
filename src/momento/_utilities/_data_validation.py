from typing import Optional, Union

from grpc.aio import Metadata

from .. import errors


DEFAULT_STRING_CONVERSION_ERROR = "Could not decode bytes to UTF-8"


def _make_metadata(cache_name: str) -> Metadata:
    return Metadata(("cache", cache_name))  # type: ignore[misc]


def _validate_cache_name(cache_name: str) -> None:
    if cache_name is None or not isinstance(cache_name, str):
        raise errors.InvalidArgumentError("Cache name must be a non-empty string")


def _as_bytes(
    data: Union[str, bytes],
    error_message: Optional[str] = DEFAULT_STRING_CONVERSION_ERROR,
) -> bytes:
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, bytes):
        return data
    raise errors.InvalidArgumentError(error_message + str(type(data)))


def _validate_ttl(ttl_seconds: int) -> None:
    if not isinstance(ttl_seconds, int) or ttl_seconds < 0:
        raise errors.InvalidArgumentError("TTL Seconds must be a non-negative integer")


def _validate_ttl_minutes(ttl_minutes: int) -> None:
    if not isinstance(ttl_minutes, int) or ttl_minutes < 0:
        raise errors.InvalidArgumentError("TTL Minutes must be a non-negative integer")


def _validate_request_timeout(request_timeout_ms: Optional[int]) -> None:
    if request_timeout_ms is None:
        return
    if not isinstance(request_timeout_ms, int) or request_timeout_ms <= 0:
        raise errors.InvalidArgumentError("Request timeout must be greater than zero.")
