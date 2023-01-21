from datetime import timedelta
from typing import Awaitable, Callable, List, Optional, Tuple, TypeVar, Union

from grpc.aio import Metadata
from momento_wire_types.cacheclient_pb2 import (
    Miss,
    _DeleteRequest,
    _DeleteResponse,
    _GetRequest,
    _GetResponse,
    _SetRequest,
    _SetResponse,
)

from momento import logs
from momento.config import Configuration
from momento.errors import SdkException, convert_error
from momento.internal._utilities import _as_bytes, _validate_cache_name, _validate_ttl
from momento.responses import (
    CacheDelete,
    CacheDeleteResponse,
    CacheGet,
    CacheGetResponse,
    CacheSet,
    CacheSetResponse,
)

TResponse = TypeVar("TResponse")
TGeneratedRequest = TypeVar("TGeneratedRequest")
TGeneratedResponse = TypeVar("TGeneratedResponse")
TExecuteResult = TypeVar("TExecuteResult")
TMomentoResponse = TypeVar("TMomentoResponse")


_logger = logs.logger
_DEFAULT_DEADLINE = timedelta(seconds=5)


def wrap_with_error_handling(
    cache_name: str,
    request_type: str,
    prepare_request_fn: Callable[[], TGeneratedRequest],
    execute_request_fn: Callable[[TGeneratedRequest], TGeneratedResponse],
    response_fn: Callable[[TGeneratedRequest, TGeneratedResponse], TMomentoResponse],
    error_fn: Callable[[SdkException], TMomentoResponse],
    metadata: List[Tuple[str, str]],
) -> TMomentoResponse:
    try:
        _validate_cache_name(cache_name)
        req = prepare_request_fn()
        resp = execute_request_fn(req)
        return response_fn(req, resp)
    except Exception as e:
        _logger.warning("%s failed with exception: %s", request_type, e)
        return error_fn(convert_error(e, metadata))


async def wrap_async_with_error_handling(
    cache_name: str,
    request_type: str,
    prepare_request_fn: Callable[[], TGeneratedRequest],
    execute_request_fn: Callable[[TGeneratedRequest], Awaitable[TGeneratedResponse]],
    response_fn: Callable[[TGeneratedRequest, TGeneratedResponse], TMomentoResponse],
    error_fn: Callable[[SdkException], TMomentoResponse],
    metadata: Metadata,
) -> TMomentoResponse:
    try:
        _validate_cache_name(cache_name)
        req = prepare_request_fn()
        resp = await execute_request_fn(req)
        return response_fn(req, resp)
    except Exception as e:
        _logger.warning("%s failed with exception: %s", request_type, e)
        return error_fn(convert_error(e, metadata))


def prepare_set_request(
    key: Union[str, bytes],
    value: Union[str, bytes],
    ttl: Optional[timedelta],
    default_ttl: timedelta,
) -> _SetRequest:
    _logger.log(logs.TRACE, "Issuing a set request with key %s", str(key))
    item_ttl = default_ttl if ttl is None else ttl
    _validate_ttl(item_ttl)
    set_request = _SetRequest()
    set_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
    set_request.cache_body = _as_bytes(value, "Unsupported type for value: ")
    set_request.ttl_milliseconds = int(item_ttl.total_seconds() * 1000)
    return set_request


def construct_set_response(req: _SetRequest, resp: _SetResponse) -> CacheSetResponse:
    _logger.log(logs.TRACE, "Set succeeded for key: %s", str(req.cache_key))
    return CacheSet.Success()


def prepare_get_request(key: Union[str, bytes]) -> _GetRequest:
    _logger.log(logs.TRACE, "Issuing a Get request with key %s", str(key))
    get_request = _GetRequest()
    get_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
    return get_request


def construct_get_response(req: _GetRequest, resp: _GetResponse) -> CacheGetResponse:
    _logger.log(logs.TRACE, "Received a get response for %s", str(req.cache_key))
    if resp.result == Miss:
        return CacheGet.Miss()
    return CacheGet.Hit(resp.cache_body)


def prepare_delete_request(key: Union[str, bytes]) -> _DeleteRequest:
    _logger.log(logs.TRACE, "Issuing a Delete request with key %s", str(key))
    delete_request = _DeleteRequest()
    delete_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
    return delete_request


def construct_delete_response(req: _DeleteRequest, resp: _DeleteResponse) -> CacheDeleteResponse:
    _logger.log(logs.TRACE, "Received a delete response for %s", str(req.cache_key))
    return CacheDelete.Success()


def get_default_client_deadline(configuration: Configuration) -> timedelta:
    return configuration.get_transport_strategy().get_grpc_configuration().get_deadline() or _DEFAULT_DEADLINE
