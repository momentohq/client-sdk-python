from grpc.aio import Metadata

from .. import errors


def _make_metadata(cache_name) -> Metadata:
    return Metadata(('cache', cache_name))


def _validate_cache_name(cache_name):
    if not cache_name or not isinstance(cache_name, str):
        raise errors.InvalidArgumentError('Cache name must be a non-None value with `str` type')


def _as_bytes(data, error_message):
    if isinstance(data, str):
        return data.encode('utf-8')
    if isinstance(data, bytes):
        return data
    raise errors.InvalidArgumentError(error_message + str(type(data)))


def _validate_ttl(ttl_seconds):
    if not isinstance(ttl_seconds, int) or ttl_seconds < 0:
        raise errors.InvalidArgumentError(
            'TTL Seconds must be a non-negative integer')
