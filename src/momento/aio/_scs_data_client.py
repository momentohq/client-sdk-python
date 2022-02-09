import asyncio
import functools
from typing import Union, Optional, List
from momento_wire_types.cacheclient_pb2 import _GetRequest
from momento_wire_types.cacheclient_pb2 import _SetRequest

from .. import cache_operation_types as cache_sdk_ops
from .. import _cache_service_errors_converter
from .. import _momento_logger
from . import _scs_grpc_manager
from .._utilities._data_validation import (
    _as_bytes,
    _validate_ttl,
    _make_metadata,
    _validate_cache_name,
    _validate_multi_op_list,
)

_DEFAULT_DEADLINE_SECONDS = 5.0  # 5 seconds


class _ScsDataClient:
    """Internal"""

    def __init__(
        self,
        auth_token: str,
        endpoint: str,
        default_ttl_seconds: int,
        operation_timeout_ms: Optional[int],
    ):
        self._default_deadline_seconds = (
            _DEFAULT_DEADLINE_SECONDS
            if not operation_timeout_ms
            else operation_timeout_ms / 1000.0
        )
        self._grpc_manager = _scs_grpc_manager._DataGrpcManager(auth_token, endpoint)
        _validate_ttl(default_ttl_seconds)
        self._default_ttlSeconds = default_ttl_seconds

    async def set(
        self,
        cache_name: str,
        key: Union[str, bytes],
        value: Union[str, bytes],
        ttl_seconds: Optional[int],
    ) -> cache_sdk_ops.CacheSetResponse:
        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f"Issuing a set request with key {key}")
            item_ttl_seconds = (
                self._default_ttlSeconds if ttl_seconds is None else ttl_seconds
            )
            _validate_ttl(item_ttl_seconds)
            set_request = _SetRequest()
            set_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
            set_request.cache_body = _as_bytes(value, "Unsupported type for value: ")
            set_request.ttl_milliseconds = item_ttl_seconds * 1000
            response = await self._grpc_manager.async_stub().Set(
                set_request,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            _momento_logger.debug(f"Set succeeded for key: {key}")
            return cache_sdk_ops.CacheSetResponse(response, set_request.cache_body)
        except Exception as e:
            _momento_logger.debug(f"Set failed for {key} with response: {e}")
            raise _cache_service_errors_converter.convert(e)

    async def multi_set(
        self,
        cache_name: str,
        set_operations: List[cache_sdk_ops.CacheMultiSetOperation],
    ) -> cache_sdk_ops.CacheMultiSetResponse:

        _validate_multi_op_list(set_operations)
        _validate_cache_name(cache_name)

        async def _execute_op(request):
            """Wrapper func for async grpc set call"""
            return await self._grpc_manager.async_stub().Set(
                request, metadata=_make_metadata(cache_name)
            )

        # Will hold which tasks have succeeded or failed to return to caller
        failed_ops = []
        successful_ops = []

        def _handle_task_result(
            t: asyncio.Task,
            key: Union[bytes, str],
            value: Union[bytes, str],
        ) -> None:
            """
            Set sub operation callback handler. Pushes result to proper return list and handles some logging for viz.
            """
            try:
                successful_ops.append(
                    cache_sdk_ops.CacheSetResponse(task.result(), key, value)
                )
            except asyncio.CancelledError:
                pass  # Task cancellation should not be logged as an error.
            except Exception as er:
                _momento_logger.debug(
                    f"multi-set sub command failed with "
                    f"error: {er} "
                    f"key: {key} "
                    f"task={t.get_name()}"
                )
                failed_ops.append(cache_sdk_ops.CacheSetResponse(None, key, value))

        try:
            request_promises = set()
            for op in set_operations:
                item_ttl_seconds = (
                    self._default_ttlSeconds
                    if op.ttl_seconds is None
                    else op.ttl_seconds
                )
                _validate_ttl(item_ttl_seconds)
                set_request = _SetRequest()
                set_request.cache_key = _as_bytes(op.key, "Unsupported type for key: ")
                set_request.cache_body = _as_bytes(
                    op.value, "Unsupported type for value: "
                )
                set_request.ttl_milliseconds = item_ttl_seconds * 1000
                task = asyncio.create_task(
                    _execute_op(set_request),
                )
                task.add_done_callback(
                    functools.partial(
                        _handle_task_result,
                        key=set_request.cache_key,
                        value=set_request.cache_body,
                    )
                )
                request_promises.add(task)

            _ = await asyncio.gather(
                *request_promises,
                return_exceptions=True,
                # When set to True exceptions are treated the same as successful results, and aggregated in the
                # result list. We want to try and make sure all requests have chance to finish.
            )
            _momento_logger.debug(
                f"multi-set succeeded "
                f"successful_op_count={len(successful_ops)} "
                f"failed_op_count={len(failed_ops)}"
            )
            return cache_sdk_ops.CacheMultiSetResponse(
                successful_responses=successful_ops, failed_responses=failed_ops
            )

        except Exception as e:
            _momento_logger.debug(f"multi-set failed with error: {e}")
            # re-raise any error caught here is fatal error with overall handling of request objects
            raise _cache_service_errors_converter.convert(e)

    async def get(
        self, cache_name: str, key: Union[str, bytes]
    ) -> cache_sdk_ops.CacheGetResponse:

        _validate_cache_name(cache_name)
        try:
            _momento_logger.debug(f"Issuing a get request with key {key}")
            get_request = _GetRequest()
            get_request.cache_key = _as_bytes(key, "Unsupported type for key: ")
            response = await self._grpc_manager.async_stub().Get(
                get_request,
                metadata=_make_metadata(cache_name),
                timeout=self._default_deadline_seconds,
            )
            _momento_logger.debug(f"Received a get response for {key}")
            return cache_sdk_ops.CacheGetResponse(response)
        except Exception as e:
            _momento_logger.debug(f"Get failed for {key} with response: {e}")
            raise _cache_service_errors_converter.convert(e)

    async def multi_get(
        self,
        cache_name: str,
        get_operations: List[cache_sdk_ops.CacheMultiGetOperation],
    ) -> cache_sdk_ops.CacheMultiGetResponse:
        _validate_multi_op_list(get_operations)
        _validate_cache_name(cache_name)
        try:
            rsp_promises = set()
            for op in get_operations:
                get_request = _GetRequest()
                get_request.cache_key = _as_bytes(op.key, "Unsupported type for key: ")
                rsp_promises.add(
                    self._grpc_manager.async_stub().Get(
                        get_request, metadata=_make_metadata(cache_name)
                    )
                )

            responses = []
            for pending_response in rsp_promises:
                # await one at a time to keep ordered for user
                completed_request = await pending_response
                # Receive the returned result of current user
                responses.append(cache_sdk_ops.CacheGetResponse(completed_request))
            _momento_logger.debug(f"multi_get succeeded")
            responses.reverse()
            return cache_sdk_ops.CacheMultiGetResponse(responses)
        except Exception as e:
            _momento_logger.debug(f"multi_get failed with response: {e}")
            raise _cache_service_errors_converter.convert(e)

    async def close(self) -> None:
        await self._grpc_manager.close()
