from typing import Awaitable, Callable, Optional, TypeVar, Union

from momento_wire_types.cacheclient_pb2 import (
    _DeleteRequest,
    _DeleteResponse,
    _GetRequest,
    _GetResponse,
    _SetRequest,
    _SetResponse,
)

from momento import _cache_service_errors_converter, cache_operation_types, logs
from momento._utilities._data_validation import (
    _as_bytes,
    _validate_cache_name,
    _validate_ttl,
)

TResponse = TypeVar("TResponse")
TGeneratedRequest = TypeVar("TGeneratedRequest")
TGeneratedResponse = TypeVar("TGeneratedResponse")
TExecuteResult = TypeVar("TExecuteResult")
TMomentoResponse = TypeVar("TMomentoResponse")

_logger = logs.logger


def wrap_with_error_handling(
    cache_name: str,
    request_type: str,
    prepare_request_fn: Callable[[], TGeneratedRequest],
    execute_request_fn: Callable[[TGeneratedRequest], TGeneratedResponse],
    response_fn: Callable[[TGeneratedRequest, TGeneratedResponse], TMomentoResponse],
) -> TMomentoResponse:
    _validate_cache_name(cache_name)
    try:
        req = prepare_request_fn()
        resp = execute_request_fn(req)
        return response_fn(req, resp)
    except Exception as e:
        _logger.warning("%s failed with exception: %s", request_type, e)
        raise _cache_service_errors_converter.convert(e)


async def wrap_async_with_error_handling(
    cache_name: str,
    request_type: str,
    prepare_request_fn: Callable[[], TGeneratedRequest],
    execute_request_fn: Callable[[TGeneratedRequest], Awaitable[TGeneratedResponse]],
    response_fn: Callable[[TGeneratedRequest, TGeneratedResponse], TMomentoResponse],
) -> TMomentoResponse:
    _validate_cache_name(cache_name)
    try:
        req = prepare_request_fn()
        resp = await execute_request_fn(req)
        return response_fn(req, resp)
    except Exception as e:
        _logger.warning("%s failed with exception: %s", request_type, e)
        raise _cache_service_errors_converter.convert(e)


def prepare_set_request(
    key: Union[str, bytes],
    value: Union[str, bytes],
    ttl_seconds: Optional[int],
    default_ttl_seconds: int,
) -> _GetRequest:
    _logger.log(logs.TRACE, "Issuing a set request with key %s", str(key))
    item_ttl_seconds = default_ttl_seconds if ttl_seconds is None else ttl_seconds
    _validate_ttl(item_ttl_seconds)
    set_request = _SetRequest()
    set_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
    set_request.cache_body = _as_bytes(value, "Unsupported type for value: ")
    set_request.ttl_milliseconds = item_ttl_seconds * 1000
    return set_request


def construct_set_response(req: _SetRequest, resp: _SetResponse) -> cache_operation_types.CacheSetResponse:
    _logger.log(logs.TRACE, "Set succeeded for key: %s", str(req.cache_key))
    return cache_operation_types.CacheSetResponse(req.cache_key, req.cache_body)


def prepare_get_request(key: Union[str, bytes]) -> _GetRequest:
    _logger.log(logs.TRACE, "Issuing a Get request with key %s", str(key))
    get_request = _GetRequest()
    get_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
    return get_request


def construct_get_response(req: _GetRequest, resp: _GetResponse) -> cache_operation_types.CacheGetResponse:
    _logger.log(logs.TRACE, "Received a get response for %s", str(req.cache_key))
    return cache_operation_types.CacheGetResponse.from_grpc_response(resp)


def prepare_delete_request(key: Union[str, bytes]) -> _DeleteRequest:
    _logger.log(logs.TRACE, "Issuing a Delete request with key %s", str(key))
    delete_request = _DeleteRequest()
    delete_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
    return delete_request


def construct_delete_response(req: _DeleteRequest, resp: _DeleteResponse) -> cache_operation_types.CacheDeleteResponse:
    _logger.log(logs.TRACE, "Received a delete response for %s", str(req.cache_key))
    return cache_operation_types.CacheDeleteResponse()
