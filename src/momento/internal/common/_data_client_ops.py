from datetime import timedelta
from typing import Awaitable, Callable, List, Tuple, TypeVar

from grpc.aio import Metadata

from momento import logs
from momento.config import Configuration
from momento.errors import SdkException, convert_error
from momento.internal._utilities import _validate_cache_name

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


def get_default_client_deadline(configuration: Configuration) -> timedelta:
    return configuration.get_transport_strategy().get_grpc_configuration().get_deadline() or _DEFAULT_DEADLINE
